package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Same-origin (no Origin header) requests are allowed; cross-origin
		// requests must come from the CORS allowlist.
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		return isAllowedOrigin(origin)
	},
}

// inbound message types
const (
	msgCreateRoom        = "create_room"
	msgRequestJoin       = "request_join"
	msgJoinRequestAnswer = "join_request_answer"
	msgCancelJoinRequest = "cancel_join_request"
	msgTransferRequest   = "transfer_request"
	msgTransferResponse  = "transfer_response"
	msgTransferCancel    = "transfer_cancel"
)

// outbound message types
const (
	msgRegistered              = "registered"
	msgRoomCreated             = "room_created"
	msgJoinRequestReceived     = "join_request_received"
	msgJoinRequestDeclined     = "join_request_declined"
	msgJoinAllowed             = "join_allowed"
	msgTransferRequestReceived = "transfer_request_received"
	msgTransferAccepted        = "transfer_accepted"
	msgTransferDeclined        = "transfer_declined"
	msgTransferReady           = "transfer_ready"
	msgTransferComplete        = "transfer_complete"
	msgTransferFailed          = "transfer_failed"
	msgError                   = "error"
)

// inbound is a permissive envelope used for dispatch and per-type decoding.
type inbound struct {
	Type     string `json:"type"`
	RoomCode string `json:"room_code,omitempty"`
	Allow    *bool  `json:"allow,omitempty"`
	Accept   *bool  `json:"accept,omitempty"`
	FileName string `json:"file_name,omitempty"`
	Size     int64  `json:"size,omitempty"`
}

func errPacket(reason string) map[string]any {
	return map[string]any{"type": msgError, "reason": reason}
}

// handleWS upgrades the HTTP connection, registers a session and dispatches
// incoming messages until the socket closes.
func (h *Hub) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade: %v", err)
		return
	}

	sid := uuid.NewString()
	client := &Client{SessionID: sid, Conn: conn}
	h.addClient(client)
	defer h.cleanupSession(sid)

	if err := client.WriteJSON(map[string]any{
		"type":       msgRegistered,
		"session_id": sid,
	}); err != nil {
		log.Printf("ws write registered: %v", err)
		return
	}

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg inbound
		if err := json.Unmarshal(raw, &msg); err != nil {
			_ = client.WriteJSON(errPacket("invalid_json"))
			continue
		}
		h.dispatch(client, &msg)
	}
}

func (h *Hub) dispatch(c *Client, m *inbound) {
	switch m.Type {
	case msgCreateRoom:
		h.onCreateRoom(c)
	case msgRequestJoin:
		h.onRequestJoin(c, m.RoomCode)
	case msgJoinRequestAnswer:
		allow := m.Allow != nil && *m.Allow
		h.onJoinRequestAnswer(c, m.RoomCode, allow)
	case msgCancelJoinRequest:
		h.onCancelJoinRequest(c, m.RoomCode)
	case msgTransferRequest:
		h.onTransferRequest(c, m.RoomCode, m.FileName, m.Size)
	case msgTransferResponse:
		accept := m.Accept != nil && *m.Accept
		h.onTransferResponse(c, m.RoomCode, accept)
	case msgTransferCancel:
		h.onTransferCancel(c, m.RoomCode)
	default:
		_ = c.WriteJSON(errPacket("unknown_message_type"))
	}
}

func (h *Hub) onCreateRoom(c *Client) {
	if h.userInAnyRoom(c.SessionID) {
		_ = c.WriteJSON(errPacket("already_in_room"))
		return
	}
	if h.hasPending(c.SessionID) {
		_ = c.WriteJSON(errPacket("active_request_exists"))
		return
	}
	room, err := h.createRoom(c.SessionID)
	if err != nil {
		_ = c.WriteJSON(errPacket("internal_error"))
		return
	}
	_ = c.WriteJSON(map[string]any{
		"type":      msgRoomCreated,
		"room_code": room.Code,
	})
}

func (h *Hub) onRequestJoin(c *Client, code string) {
	if code == "" {
		_ = c.WriteJSON(errPacket("missing_room_code"))
		return
	}
	if h.userInAnyRoom(c.SessionID) {
		_ = c.WriteJSON(errPacket("already_in_room"))
		return
	}
	if h.hasPending(c.SessionID) {
		_ = c.WriteJSON(errPacket("active_request_exists"))
		return
	}
	room, ok := h.getRoom(code)
	if !ok {
		_ = c.WriteJSON(errPacket("room_not_found"))
		return
	}
	// Snapshot owner under the rooms lock to avoid races with deletion.
	h.roomsMu.RLock()
	if len(room.Members) >= 2 {
		h.roomsMu.RUnlock()
		_ = c.WriteJSON(errPacket("room_full"))
		return
	}
	owner := room.Members[0]
	h.roomsMu.RUnlock()

	ownerClient, ok := h.getClient(owner)
	if !ok {
		_ = c.WriteJSON(errPacket("room_owner_unavailable"))
		return
	}

	h.setPending(c.SessionID, code)
	_ = ownerClient.WriteJSON(map[string]any{
		"type":            msgJoinRequestReceived,
		"peer_session_id": c.SessionID,
		"room_code":       code,
	})
}

func (h *Hub) onJoinRequestAnswer(c *Client, code string, allow bool) {
	if code == "" {
		_ = c.WriteJSON(errPacket("missing_room_code"))
		return
	}
	room, ok := h.getRoom(code)
	if !ok {
		_ = c.WriteJSON(errPacket("room_not_found"))
		return
	}
	// Only the owner may answer.
	h.roomsMu.RLock()
	if len(room.Members) == 0 || room.Members[0] != c.SessionID {
		h.roomsMu.RUnlock()
		_ = c.WriteJSON(errPacket("not_room_owner"))
		return
	}
	h.roomsMu.RUnlock()

	requesters := h.pendingRequestersFor(code)
	if len(requesters) == 0 {
		_ = c.WriteJSON(errPacket("no_pending_request"))
		return
	}
	// Answer all pending requesters for this room with the same verdict.
	// In practice there will normally be at most one but we don't enforce it.
	for _, reqSid := range requesters {
		h.clearPending(reqSid)
		reqClient, ok := h.getClient(reqSid)
		if !ok {
			continue
		}
		if !allow {
			_ = reqClient.WriteJSON(map[string]any{
				"type":      msgJoinRequestDeclined,
				"room_code": code,
			})
			continue
		}
		if !h.addMember(code, reqSid) {
			_ = reqClient.WriteJSON(errPacket("room_full"))
			continue
		}
		_ = reqClient.WriteJSON(map[string]any{
			"type":            msgJoinAllowed,
			"room_code":       code,
			"peer_session_id": c.SessionID,
		})
	}
}

func (h *Hub) onCancelJoinRequest(c *Client, code string) {
	prev, ok := h.clearPending(c.SessionID)
	if !ok {
		return
	}
	if code != "" && prev != code {
		// The user asked to cancel a request for a different room than
		// what we have on file — restore and report.
		h.setPending(c.SessionID, prev)
		_ = c.WriteJSON(errPacket("no_matching_pending_request"))
	}
}

// cleanupSession removes a session from all maps when its socket dies and
// notifies any peers / pending counterparts.
func (h *Hub) cleanupSession(sid string) {
	// Abort any in-flight transfer this session is part of first, so the
	// peer learns the cause before we tear down the rest of the state.
	h.abortTransfersFor(sid, "peer_disconnected")
	h.removeClient(sid)
	h.clearPending(sid)

	// If the user owned a room or was a member, dismantle accordingly.
	h.roomsMu.Lock()
	var ownedCode string
	var roomToUpdate *Room
	for code, r := range h.rooms {
		if len(r.Members) > 0 && r.Members[0] == sid {
			ownedCode = code
			break
		}
		for i, m := range r.Members {
			if m == sid {
				r.Members = append(r.Members[:i], r.Members[i+1:]...)
				roomToUpdate = r
				break
			}
		}
	}
	if ownedCode != "" {
		delete(h.rooms, ownedCode)
	}
	h.roomsMu.Unlock()

	// Drop any pending requests aimed at the destroyed room.
	if ownedCode != "" {
		for _, reqSid := range h.pendingRequestersFor(ownedCode) {
			h.clearPending(reqSid)
			if rc, ok := h.getClient(reqSid); ok {
				_ = rc.WriteJSON(map[string]any{
					"type":      msgJoinRequestDeclined,
					"room_code": ownedCode,
					"reason":    "owner_disconnected",
				})
			}
		}
	}
	_ = roomToUpdate // reserved for future peer-disconnect notifications
}
