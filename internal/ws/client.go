package ws

import "github.com/gorilla/websocket"

type Client struct {
	Conn           *websocket.Conn
	UserID         string
	ConversationID string
	Send           chan []byte
}
