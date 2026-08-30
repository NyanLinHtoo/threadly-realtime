package main

import (
	"chat-app-realtime/internal/auth"
	"chat-app-realtime/internal/pubsub"
	"chat-app-realtime/internal/ws"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var allowedOrigin = func() string {
	if origin := os.Getenv("FRONTEND_URL"); origin != "" {
		return origin
	}
	return "http://localhost:3000"
}()

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return r.Header.Get("Origin") == allowedOrigin
	},
}

func handleWebSocket(hub *ws.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		conversationID := r.URL.Query().Get("conversationId")
		if conversationID == "" {
			http.Error(w, "missing conversationId", http.StatusUnauthorized)
			return
		}

		secret := os.Getenv("JWT_SECRET")

		tokenString := r.URL.Query().Get("token")
		if tokenString == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
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

func handleNotificationWebSocket(hub *ws.NotificationHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("token")
		var tokenString string
		if err == nil {
			tokenString = cookie.Value
		} else {
			tokenString = r.URL.Query().Get("token")
		}

		if tokenString == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
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
			Conn:   conn,
			UserID: claims.UserID,
			Send:   make(chan []byte, 256),
			Hub:    hub,
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

	notificationHub := ws.NewNotificationHub()
	go notificationHub.Run()

	redisADDR := os.Getenv("REDIS_ADDR")
	if redisADDR == "" {
		log.Fatal("REDIS_ADDR environment variable is required")
	}

	opts, err := redis.ParseURL(redisADDR)
	if err != nil {
		log.Fatal("invalid Redis URL:", err)
	}

	redisClient := redis.NewClient(opts)

	go pubsub.SubscribeAndBroadcast(redisClient, hub)
	go pubsub.SubscribeAndNotify(redisClient, notificationHub)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/ws", handleWebSocket(hub))
	http.HandleFunc("/ws/notifications", handleNotificationWebSocket(notificationHub))

	log.Println("Server starting on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
