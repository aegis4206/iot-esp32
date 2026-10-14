package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"iot-backend/internal/database"
	"iot-backend/internal/handler"
	"iot-backend/internal/model"
	"iot-backend/internal/repository"
	"iot-backend/internal/router"
	"log"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func NewExceedLimitTracker() *model.ExceedLimitTracker {
	return &model.ExceedLimitTracker{
		Devices: make(map[string]int),
	}
}

func main() {
	// 初始化資料庫連線
	database.InitDB()
	defer database.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tempLogRepo := repository.NewTemperatureLogRepository(database.GetDB())
	tcsDeviceRepo := repository.NewTCSDeviceRepository(database.GetDB())
	tracker := NewExceedLimitTracker()

	// 啟動 MQTT Broker
	handler.StartMQTTBroker(ctx, tempLogRepo, tcsDeviceRepo, tracker)

	// 設定 Gin Web Server
	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		// AllowOrigins:    []string{"http://localhost:9000"},
		AllowMethods:    []string{"*"},
		AllowWebSockets: true,
	}))

	router.Setup(r)

	srv := &http.Server{
		Addr:    ":9000",
		Handler: r,
	}

	go func() {
		log.Println("[HTTP Server] listening on :9000")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[HTTP Server] Run error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[Main] shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[HTTP Server] forced shutdown: %v", err)
	}

	log.Println("[Main] server exited gracefully")
}
