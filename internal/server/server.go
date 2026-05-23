package server

import (
	"log"
	"net/http"
)

// allowedOrigins is the closed allowlist for CORS and WS Origin checks.
var allowedOrigins = map[string]struct{}{
	"https://send.rian.moe": {},
	"http://localhost:5173": {},
}

func isAllowedOrigin(origin string) bool {
	_, ok := allowedOrigins[origin]
	return ok
}

// withCORS wraps h with an allowlist-based CORS handler. Preflights are
// answered directly; non-preflight responses get the matching
// Access-Control-Allow-Origin echoed back (or no header at all if the
// origin is disallowed, which the browser will treat as a CORS failure).
func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && isAllowedOrigin(origin) {
			hdr := w.Header()
			hdr.Set("Access-Control-Allow-Origin", origin)
			hdr.Set("Vary", "Origin")
			hdr.Set("Access-Control-Allow-Credentials", "true")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			hdr := w.Header()
			hdr.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			if reqHdrs := r.Header.Get("Access-Control-Request-Headers"); reqHdrs != "" {
				hdr.Set("Access-Control-Allow-Headers", reqHdrs)
			} else {
				hdr.Set("Access-Control-Allow-Headers", "Content-Type")
			}
			hdr.Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// Run starts the HTTP server on addr and blocks until it exits.
func Run(addr string) error {
	hub := NewHub()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.handleWS)
	mux.HandleFunc("GET /download/{code}/{filename}", hub.handleDownload)
	mux.HandleFunc("POST /upload/{code}/{filename}", hub.handleUpload)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("aki-sender api listening on %s", addr)
	return http.ListenAndServe(addr, withCORS(mux))
}
