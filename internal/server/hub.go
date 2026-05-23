package server

import (
	"crypto/rand"
	"sync"

	"github.com/gorilla/websocket"
)

// Room is a sharing session identified by a 6-char alphanumeric Code.
// It can hold up to two peers: the owner (creator) and an accepted joiner.
type Room struct {
	Code    string
	Members []string // session IDs; Members[0] is always the owner
}

// Client wraps a websocket connection with a write mutex so we can safely
// write to it from multiple goroutines.
type Client struct {
	SessionID string
	Conn      *websocket.Conn
	writeMu   sync.Mutex
}

// WriteJSON serializes v as JSON and writes it to the underlying connection
// while holding the per-client write mutex.
func (c *Client) WriteJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.WriteJSON(v)
}

// Hub keeps the global state of the service.
//
// Three maps, each guarded by its own RWMutex:
//   - clients:      session_id -> *Client (websocket)
//   - rooms:        room_code  -> *Room
//   - pendingReqs:  session_id -> room_code (the room a user is currently
//     requesting to join; "active request" tracker)
type Hub struct {
	clientsMu sync.RWMutex
	clients   map[string]*Client

	roomsMu sync.RWMutex
	rooms   map[string]*Room

	pendingMu   sync.RWMutex
	pendingReqs map[string]string

	transfersMu sync.RWMutex
	transfers   map[string]*Transfer
}

func NewHub() *Hub {
	return &Hub{
		clients:     make(map[string]*Client),
		rooms:       make(map[string]*Room),
		pendingReqs: make(map[string]string),
		transfers:   make(map[string]*Transfer),
	}
}

// peerOf returns the other member of the room sid is in. Returns ("", false)
// if the room does not exist, sid is not a member, or the room only has one
// member.
func (h *Hub) peerOf(roomCode, sid string) (string, bool) {
	h.roomsMu.RLock()
	defer h.roomsMu.RUnlock()
	r, ok := h.rooms[roomCode]
	if !ok {
		return "", false
	}
	var found bool
	var other string
	for _, m := range r.Members {
		if m == sid {
			found = true
		} else {
			other = m
		}
	}
	if !found || other == "" {
		return "", false
	}
	return other, true
}

// --- clients ---

func (h *Hub) addClient(c *Client) {
	h.clientsMu.Lock()
	h.clients[c.SessionID] = c
	h.clientsMu.Unlock()
}

func (h *Hub) getClient(sid string) (*Client, bool) {
	h.clientsMu.RLock()
	c, ok := h.clients[sid]
	h.clientsMu.RUnlock()
	return c, ok
}

func (h *Hub) removeClient(sid string) {
	h.clientsMu.Lock()
	delete(h.clients, sid)
	h.clientsMu.Unlock()
}

// --- rooms ---

func (h *Hub) getRoom(code string) (*Room, bool) {
	h.roomsMu.RLock()
	r, ok := h.rooms[code]
	h.roomsMu.RUnlock()
	return r, ok
}

// userInAnyRoom reports whether sid is a member of any room.
func (h *Hub) userInAnyRoom(sid string) bool {
	h.roomsMu.RLock()
	defer h.roomsMu.RUnlock()
	for _, r := range h.rooms {
		for _, m := range r.Members {
			if m == sid {
				return true
			}
		}
	}
	return false
}

// createRoom generates a unique 6-char code and registers a new room owned
// by sid. Returns the created room.
func (h *Hub) createRoom(sid string) (*Room, error) {
	h.roomsMu.Lock()
	defer h.roomsMu.Unlock()
	var code string
	for {
		c, err := randomCode(6)
		if err != nil {
			return nil, err
		}
		if _, exists := h.rooms[c]; !exists {
			code = c
			break
		}
	}
	r := &Room{Code: code, Members: []string{sid}}
	h.rooms[code] = r
	return r, nil
}

func (h *Hub) deleteRoom(code string) {
	h.roomsMu.Lock()
	delete(h.rooms, code)
	h.roomsMu.Unlock()
}

// addMember appends sid to the room's members. Caller must hold no locks.
// Returns false if the room is full or missing.
func (h *Hub) addMember(code, sid string) bool {
	h.roomsMu.Lock()
	defer h.roomsMu.Unlock()
	r, ok := h.rooms[code]
	if !ok || len(r.Members) >= 2 {
		return false
	}
	r.Members = append(r.Members, sid)
	return true
}

// --- pending requests ---

func (h *Hub) hasPending(sid string) bool {
	h.pendingMu.RLock()
	_, ok := h.pendingReqs[sid]
	h.pendingMu.RUnlock()
	return ok
}

func (h *Hub) setPending(sid, code string) {
	h.pendingMu.Lock()
	h.pendingReqs[sid] = code
	h.pendingMu.Unlock()
}

func (h *Hub) clearPending(sid string) (string, bool) {
	h.pendingMu.Lock()
	defer h.pendingMu.Unlock()
	code, ok := h.pendingReqs[sid]
	if ok {
		delete(h.pendingReqs, sid)
	}
	return code, ok
}

// pendingRequestersFor returns all session IDs whose pending request targets
// the given room code.
func (h *Hub) pendingRequestersFor(code string) []string {
	h.pendingMu.RLock()
	defer h.pendingMu.RUnlock()
	var out []string
	for sid, c := range h.pendingReqs {
		if c == code {
			out = append(out, sid)
		}
	}
	return out
}

// --- helpers ---

const codeAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomCode(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	out := make([]byte, n)
	for i, b := range buf {
		out[i] = codeAlphabet[int(b)%len(codeAlphabet)]
	}
	return string(out), nil
}
