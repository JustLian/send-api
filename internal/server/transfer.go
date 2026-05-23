package server

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
)

// MaxFileSize is the upper bound on a single transfer's declared size.
const MaxFileSize int64 = 10 * 1024 * 1024 * 1024 // 10 GiB

// Transfer represents one in-flight (or pending) file transfer between the
// two members of a room.
//
// Lifecycle:
//
//	registered  -> sender's transfer_request validated; receiver notified
//	accepted    -> receiver's transfer_response{accept:true} processed
//	streaming   -> receiver opened GET /download; pipe established;
//	               sender notified via transfer_ready
//	finished    -> upload finished or either side errored/disconnected
type Transfer struct {
	RoomCode    string
	SenderSID   string
	ReceiverSID string
	FileName    string
	Size        int64

	mu       sync.Mutex
	accepted bool
	pipeR    *io.PipeReader
	pipeW    *io.PipeWriter
	piped    bool // true once pipe has been wired up by /download

	once sync.Once
	done chan struct{}
}

func newTransfer(roomCode, sender, receiver, name string, size int64) *Transfer {
	return &Transfer{
		RoomCode:    roomCode,
		SenderSID:   sender,
		ReceiverSID: receiver,
		FileName:    name,
		Size:        size,
		done:        make(chan struct{}),
	}
}

// finish closes the pipe (with err if non-nil), removes the transfer from
// the hub, and notifies both peers via WS. Idempotent.
//
// reason="" indicates a clean completion; any non-empty reason produces a
// transfer_failed packet with that reason.
func (t *Transfer) finish(h *Hub, reason string) {
	t.once.Do(func() {
		t.mu.Lock()
		if t.pipeW != nil {
			if reason != "" {
				_ = t.pipeW.CloseWithError(errors.New(reason))
			} else {
				_ = t.pipeW.Close()
			}
		}
		if t.pipeR != nil {
			if reason != "" {
				_ = t.pipeR.CloseWithError(errors.New(reason))
			} else {
				_ = t.pipeR.Close()
			}
		}
		t.mu.Unlock()

		h.transfersMu.Lock()
		if cur, ok := h.transfers[t.RoomCode]; ok && cur == t {
			delete(h.transfers, t.RoomCode)
		}
		h.transfersMu.Unlock()

		payload := map[string]any{
			"room_code": t.RoomCode,
			"file_name": t.FileName,
		}
		if reason == "" {
			payload["type"] = msgTransferComplete
		} else {
			payload["type"] = msgTransferFailed
			payload["reason"] = reason
		}
		if c, ok := h.getClient(t.SenderSID); ok {
			_ = c.WriteJSON(payload)
		}
		if c, ok := h.getClient(t.ReceiverSID); ok {
			_ = c.WriteJSON(payload)
		}
		close(t.done)
	})
}

// --- hub helpers ---

func (h *Hub) getTransfer(code string) (*Transfer, bool) {
	h.transfersMu.RLock()
	t, ok := h.transfers[code]
	h.transfersMu.RUnlock()
	return t, ok
}

// registerTransfer atomically inserts t if and only if no transfer is in
// progress for that room.
func (h *Hub) registerTransfer(t *Transfer) bool {
	h.transfersMu.Lock()
	defer h.transfersMu.Unlock()
	if _, exists := h.transfers[t.RoomCode]; exists {
		return false
	}
	h.transfers[t.RoomCode] = t
	return true
}

// abortTransfersFor removes any transfer the given session is part of and
// notifies the surviving peer with transfer_failed. Used during socket
// teardown.
func (h *Hub) abortTransfersFor(sid string, reason string) {
	h.transfersMu.RLock()
	var victims []*Transfer
	for _, t := range h.transfers {
		if t.SenderSID == sid || t.ReceiverSID == sid {
			victims = append(victims, t)
		}
	}
	h.transfersMu.RUnlock()
	for _, t := range victims {
		t.finish(h, reason)
	}
}

// --- WS handlers ---

func (h *Hub) onTransferRequest(c *Client, roomCode, fileName string, size int64) {
	if roomCode == "" || fileName == "" {
		_ = c.WriteJSON(errPacket("missing_field"))
		return
	}
	if size <= 0 {
		_ = c.WriteJSON(errPacket("invalid_size"))
		return
	}
	if size > MaxFileSize {
		_ = c.WriteJSON(errPacket("file_too_large"))
		return
	}
	// Reject path-y filenames so the URL form remains predictable.
	if fileName != path.Base(fileName) || strings.ContainsAny(fileName, "/\\") {
		_ = c.WriteJSON(errPacket("invalid_file_name"))
		return
	}

	peer, ok := h.peerOf(roomCode, c.SessionID)
	if !ok {
		_ = c.WriteJSON(errPacket("not_in_room"))
		return
	}
	peerClient, ok := h.getClient(peer)
	if !ok {
		_ = c.WriteJSON(errPacket("peer_unavailable"))
		return
	}

	t := newTransfer(roomCode, c.SessionID, peer, fileName, size)
	if !h.registerTransfer(t) {
		_ = c.WriteJSON(errPacket("transfer_in_progress"))
		return
	}

	_ = peerClient.WriteJSON(map[string]any{
		"type":            msgTransferRequestReceived,
		"room_code":       roomCode,
		"peer_session_id": c.SessionID,
		"file_name":       fileName,
		"size":            size,
	})
}

func (h *Hub) onTransferResponse(c *Client, roomCode string, accept bool) {
	if roomCode == "" {
		_ = c.WriteJSON(errPacket("missing_room_code"))
		return
	}
	t, ok := h.getTransfer(roomCode)
	if !ok {
		_ = c.WriteJSON(errPacket("no_active_transfer"))
		return
	}
	if t.ReceiverSID != c.SessionID {
		_ = c.WriteJSON(errPacket("not_transfer_recipient"))
		return
	}

	t.mu.Lock()
	if t.accepted {
		t.mu.Unlock()
		_ = c.WriteJSON(errPacket("already_answered"))
		return
	}
	if !accept {
		t.mu.Unlock()
		// Clean reject path: notify sender, drop transfer; no streaming
		// happened so we don't go through finish() (which would emit a
		// transfer_failed).
		h.transfersMu.Lock()
		if cur, ok := h.transfers[roomCode]; ok && cur == t {
			delete(h.transfers, roomCode)
		}
		h.transfersMu.Unlock()
		if sc, ok := h.getClient(t.SenderSID); ok {
			_ = sc.WriteJSON(map[string]any{
				"type":      msgTransferDeclined,
				"room_code": roomCode,
				"file_name": t.FileName,
			})
		}
		return
	}
	t.accepted = true
	t.mu.Unlock()

	// Tell the sender the receiver agreed. Sender still waits for
	// transfer_ready before POSTing /upload.
	if sc, ok := h.getClient(t.SenderSID); ok {
		_ = sc.WriteJSON(map[string]any{
			"type":      msgTransferAccepted,
			"room_code": roomCode,
			"file_name": t.FileName,
		})
	}
}

func (h *Hub) onTransferCancel(c *Client, roomCode string) {
	t, ok := h.getTransfer(roomCode)
	if !ok {
		return
	}
	if t.SenderSID != c.SessionID && t.ReceiverSID != c.SessionID {
		_ = c.WriteJSON(errPacket("not_transfer_participant"))
		return
	}
	t.finish(h, "cancelled_by_"+roleOf(t, c.SessionID))
}

func roleOf(t *Transfer, sid string) string {
	if t.SenderSID == sid {
		return "sender"
	}
	return "receiver"
}

// --- HTTP handlers ---

// handleDownload: GET /download/{code}/{filename}?sid=...
//
// Validates the request against the active Transfer, builds the io.Pipe,
// signals the sender that the peer is ready, and streams pipe -> response
// until upload closes the writer.
func (h *Hub) handleDownload(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	rawName := r.PathValue("filename")
	name, err := url.PathUnescape(rawName)
	if err != nil {
		http.Error(w, "bad filename", http.StatusBadRequest)
		return
	}
	sid := r.URL.Query().Get("sid")
	if sid == "" {
		http.Error(w, "missing sid", http.StatusBadRequest)
		return
	}

	t, ok := h.getTransfer(code)
	if !ok {
		http.Error(w, "no active transfer", http.StatusNotFound)
		return
	}

	t.mu.Lock()
	switch {
	case t.ReceiverSID != sid:
		t.mu.Unlock()
		http.Error(w, "not transfer recipient", http.StatusForbidden)
		return
	case !t.accepted:
		t.mu.Unlock()
		http.Error(w, "transfer not accepted", http.StatusConflict)
		return
	case t.FileName != name:
		t.mu.Unlock()
		http.Error(w, "file name mismatch", http.StatusNotFound)
		return
	case t.piped:
		t.mu.Unlock()
		http.Error(w, "download already in progress", http.StatusConflict)
		return
	}
	pr, pw := io.Pipe()
	t.pipeR = pr
	t.pipeW = pw
	t.piped = true
	sender := t.SenderSID
	fname := t.FileName
	size := t.Size
	t.mu.Unlock()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("Content-Disposition", contentDisposition(fname))
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Signal the sender that the peer is ready.
	if sc, ok := h.getClient(sender); ok {
		_ = sc.WriteJSON(map[string]any{
			"type":      msgTransferReady,
			"room_code": code,
			"file_name": fname,
		})
	}

	// Abort the transfer if the downloader goes away mid-stream.
	go func() {
		select {
		case <-r.Context().Done():
			_ = pr.CloseWithError(r.Context().Err())
		case <-t.done:
		}
	}()

	n, copyErr := io.Copy(w, pr)
	switch {
	case copyErr != nil:
		t.finish(h, "download_interrupted")
	case n != size:
		t.finish(h, "size_mismatch")
	default:
		t.finish(h, "")
	}
}

// handleUpload: POST /upload/{code}/{filename}?sid=...
//
// Streams the request body to the pipe writer set up by the download
// handler.
func (h *Hub) handleUpload(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	rawName := r.PathValue("filename")
	name, err := url.PathUnescape(rawName)
	if err != nil {
		http.Error(w, "bad filename", http.StatusBadRequest)
		return
	}
	sid := r.URL.Query().Get("sid")
	if sid == "" {
		http.Error(w, "missing sid", http.StatusBadRequest)
		return
	}

	t, ok := h.getTransfer(code)
	if !ok {
		http.Error(w, "no active transfer", http.StatusNotFound)
		return
	}

	t.mu.Lock()
	switch {
	case t.SenderSID != sid:
		t.mu.Unlock()
		http.Error(w, "not transfer sender", http.StatusForbidden)
		return
	case t.FileName != name:
		t.mu.Unlock()
		http.Error(w, "file name mismatch", http.StatusNotFound)
		return
	case !t.piped || t.pipeW == nil:
		t.mu.Unlock()
		http.Error(w, "peer not ready", http.StatusConflict)
		return
	}
	pw := t.pipeW
	size := t.Size
	t.mu.Unlock()

	// Cap the body at the declared size to defend against runaway uploads.
	body := http.MaxBytesReader(w, r.Body, size)
	defer body.Close()

	n, err := io.Copy(pw, body)
	if err != nil {
		_ = pw.CloseWithError(err)
		t.finish(h, "upload_interrupted")
		http.Error(w, "upload interrupted", http.StatusBadRequest)
		return
	}
	if n != size {
		_ = pw.CloseWithError(fmt.Errorf("short upload: %d/%d", n, size))
		t.finish(h, "size_mismatch")
		http.Error(w, "short upload", http.StatusBadRequest)
		return
	}
	if err := pw.Close(); err != nil {
		t.finish(h, "upload_close_failed")
		http.Error(w, "close failed", http.StatusInternalServerError)
		return
	}
	// Wait for the download handler to drain and finalize.
	<-t.done
	w.WriteHeader(http.StatusOK)
}

// contentDisposition produces a Content-Disposition header value that is
// safe for arbitrary UTF-8 file names (RFC 5987).
func contentDisposition(name string) string {
	quoted := strings.ReplaceAll(name, `"`, `\"`)
	return fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`,
		quoted, url.PathEscape(name))
}
