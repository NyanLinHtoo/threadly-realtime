package ws

import (
	"log"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn           *websocket.Conn
	UserID         string
	ConversationID string
	Send           chan []byte
	Hub            *Hub
}

func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister(c)
		c.Conn.Close()
	}()

	for {
		_, _, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}
		// clients don't send chat messages over this connection in our design —
		// REST handles writes. We still need to read to detect disconnects/pings.
	}
}

func (c *Client) WritePump() {
	defer c.Conn.Close()

	for message := range c.Send {
		if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
			log.Println("write error:", err)
			return
		}
	}

	c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
}
