package handler

import (
	"fmt"
	model "iot-backend/internal/model"
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	clients      = make(map[*websocket.Conn]bool)
	clientsMutex sync.RWMutex
)

var (
	latestData  *model.TemperatureLog
	latestMutex sync.RWMutex
)

func Broadcast(data model.TemperatureLog) {
	latestMutex.Lock()
	latestData = &data
	latestMutex.Unlock()

	var deviceCode, temperature string
	if data.DeviceCode != nil {
		deviceCode = *data.DeviceCode
	}
	if data.Temperature != nil {
		temperature = fmt.Sprintf("%.2f", *data.Temperature)
	}
	log.Printf("[MQTT] Received data: ID=%d DeviceCode=%s Temperature=%s RecordedAt=%s",
		data.ID, deviceCode, temperature, data.RecordedAt.Format("2006-01-02 15:04:05"))

	clientsMutex.RLock()
	if len(clients) == 0 {
		clientsMutex.RUnlock()
		return
	}
	targets := make([]*websocket.Conn, 0, len(clients))
	for conn := range clients {
		targets = append(targets, conn)
	}
	clientsMutex.RUnlock()

	var deadConns []*websocket.Conn
	for _, conn := range targets {
		if err := conn.WriteJSON(data); err != nil {
			log.Printf("[WS] Write error: %v", err)
			conn.Close()
			deadConns = append(deadConns, conn)
		}
	}

	if len(deadConns) > 0 {
		clientsMutex.Lock()
		for _, conn := range deadConns {
			delete(clients, conn)
		}
		clientsMutex.Unlock()
	}
}

func GetLatestHandler(c *gin.Context) {
	latestMutex.RLock()
	defer latestMutex.RUnlock()

	if latestData == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "No sensor data received yet",
		})
		return
	}

	c.JSON(http.StatusOK, latestData)
}

func WsHandler(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[WS] Upgrade error: %v", err)
		return
	}
	clientsMutex.Lock()
	clients[conn] = true
	clientsMutex.Unlock()
	log.Printf("[WS] Client connected. Total: %d", len(clients))

	// latestMutex.RLock()
	// if latestData != nil {
	// 	if snapshot, err := json.Marshal(latestData); err == nil {
	// 		conn.WriteMessage(websocket.TextMessage, snapshot)
	// 	}
	// }
	// latestMutex.RUnlock()

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			clientsMutex.Lock()
			delete(clients, conn)
			clientsMutex.Unlock()
			conn.Close()
			log.Printf("[WS] Client disconnected. Total: %d", len(clients))
			break
		}
	}
}
