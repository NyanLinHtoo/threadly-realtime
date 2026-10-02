package main

import (
	"chat-app-realtime/internal/auth"
	"chat-app-realtime/internal/pubsub"
	"chat-app-realtime/internal/ws"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})

	go pubsub.SubscribeAndBroadcast(redisClient, hub)
	go pubsub.SubscribeAndNotify(redisClient, notificationHub)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/ws", handleWebSocket(hub))
	http.HandleFunc("/ws/notifications", handleNotificationWebSocket(notificationHub))

	server := &http.Server{
		Addr: ":8080",
	}

	go func() {
		log.Println("Server starting on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Shutdown signal received, shutting down gracefully")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Println("Server forced to shut down:", err)
	}

	if err := redisClient.Close(); err != nil {
		log.Println("Error closing Redis client:", err)
	}

	log.Println("Cleanup complete. Exiting.")
}
