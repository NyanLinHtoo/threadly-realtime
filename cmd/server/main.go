package main

import (
	"chat-app-realtime/internal/auth"
	"chat-app-realtime/internal/ws"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // TODO: restrict this properly later
	},
}

func handleWebSocket(hub *ws.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tokenString := r.URL.Query().Get("token")
		conversationID := r.URL.Query().Get("conversationId")

		if tokenString == "" || conversationID == "" {
			http.Error(w, "missing token or conversationId", http.StatusUnauthorized)
			return
		}

		secret := os.Getenv("JWT_SECRET")
		claims, err := auth.VerifyToken(tokenString, secret)
		if err != nil {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade error:", err)
			return
		}

		client := &ws.Client{
			Conn:           conn,
			UserID:         claims.UserID,
			ConversationID: conversationID,
			Send:           make(chan []byte, 256),
			Hub:            hub,
		}

		hub.Register(client)

		go client.WritePump()
		go client.ReadPump()
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}

	hub := ws.NewHub()
	go hub.Run()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/ws", handleWebSocket(hub))

	log.Println("Server starting on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
