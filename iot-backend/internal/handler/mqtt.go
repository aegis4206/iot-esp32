package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iot-backend/internal/model"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"iot-backend/internal/repository"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

type SensorHook struct {
	mqtt.HookBase
	ctx           context.Context
	tempLogRepo   *repository.TemperatureLogRepository
	tcsDeviceRepo *repository.TCSDeviceRepository
	tracker       *model.ExceedLimitTracker
	queryCh       chan []byte
	queryMu       sync.Mutex
}

func (h *SensorHook) ID() string {
	return "sensor-hook"
}

func (h *SensorHook) Provides(b byte) bool {
	return b == mqtt.OnPublished
}

var lastUpdatedAt time.Time

var tcsDeviceMap map[string]model.TCSDeviceMapping
var tcsDeviceMapMu sync.RWMutex
var tcsDeviceMapRefreshing atomic.Bool

func buildTCSDeviceMap(devices []model.TCSDeviceMapping) map[string]model.TCSDeviceMapping {
	m := make(map[string]model.TCSDeviceMapping, len(devices))
	for _, d := range devices {
		m[d.DeviceCode] = d
	}
	return m
}

func RefreshTCSDeviceMap(ctx context.Context, tcsDeviceRepo *repository.TCSDeviceRepository) error {
	if !tcsDeviceMapRefreshing.CompareAndSwap(false, true) {
		return nil
	}
	defer tcsDeviceMapRefreshing.Store(false)

	tcsDevices, err := tcsDeviceRepo.GetTCSDevices(ctx)
	if err != nil {
		return err
	}

	newMap := buildTCSDeviceMap(tcsDevices)
	tcsDeviceMapMu.Lock()
	tcsDeviceMap = newMap
	tcsDeviceMapMu.Unlock()

	return nil
}

func CheckTemperatureLimits(ctx context.Context, record *model.TemperatureLog, tracker *model.ExceedLimitTracker) {
	tcsDeviceMapMu.RLock()
	deviceMap := tcsDeviceMap
	tcsDeviceMapMu.RUnlock()

	discordNotifyAPIURL := os.Getenv("DISCORD_NOTIFY_API_URL")
	lineNotifyAPIURL := os.Getenv("LINE_NOTIFY_API_URL")

	var lineMessages []model.NotifyRequest

	r := record
	d, ok := deviceMap[*r.DeviceCode]
	if !ok {
		return
	}

	temp := r.Temperature

	if d.UpperLimit != nil && *temp > *d.UpperLimit {
		notifyData := model.NotifyRequest{
			Type:               "up",
			DeviceCode:         d.DeviceCode,
			DeviceName:         d.DeviceName,
			ZoneName:           d.ZoneName,
			CurrentTemperature: fmt.Sprintf("%.1f", *temp),
			Limit:              fmt.Sprintf("%.1f", *d.UpperLimit),
		}
		if err := sendNotify(ctx, notifyData, discordNotifyAPIURL); err != nil {
			fmt.Printf("送出 Discord 通知失敗: %v", err)
		}
		deviceExceedCount := tracker.Update(*r.DeviceCode, true)
		if deviceExceedCount >= 12 {
			fmt.Printf("裝置 %s 已連續超過上限一小時，發送 Line 通知", *r.DeviceCode)
			lineMessages = append(lineMessages, notifyData)
			tracker.Update(*r.DeviceCode, false) // 重置計數器，避免重複通知
		}
	} else if d.LowerLimit != nil && *temp < *d.LowerLimit {
		notifyData := model.NotifyRequest{
			Type:               "low",
			DeviceCode:         d.DeviceCode,
			DeviceName:         d.DeviceName,
			ZoneName:           d.ZoneName,
			CurrentTemperature: fmt.Sprintf("%.1f", *temp),
			Limit:              fmt.Sprintf("%.1f", *d.LowerLimit),
		}
		if err := sendNotify(ctx, notifyData, discordNotifyAPIURL); err != nil {
			fmt.Printf("送出 Discord 通知失敗: %v", err)
		}
		deviceExceedCount := tracker.Update(*r.DeviceCode, true)
		if deviceExceedCount >= 12 {
			fmt.Printf("裝置 %s 已連續低於下限一小時，發送 Line 通知", *r.DeviceCode)
			lineMessages = append(lineMessages, notifyData)
			tracker.Update(*r.DeviceCode, false) // 重置計數器，避免重複通知
		}

	} else {
		tracker.Update(*r.DeviceCode, false)
	}

	if len(lineMessages) > 0 {
		for _, msg := range lineMessages {
			if err := sendNotify(ctx, msg, lineNotifyAPIURL); err != nil {
				fmt.Printf("送出 Line 通知失敗: %v", err)
			}
		}
	}
}

func IsWithinOfficeHours() bool {
	loc, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	now := time.Now().In(loc)
	weekday := now.Weekday()

	// 判斷是否為平日（週一到週五）
	isWeekday := weekday >= time.Monday && weekday <= time.Friday
	if !isWeekday {
		return false
	}

	hour := now.Hour()
	minute := now.Minute()

	nowMinutes := hour*60 + minute
	startMinutes := 8*60 + 30 // 08:30
	endMinutes := 17*60 + 30  // 17:30

	// 在 08:30~17:30 之間（含08:30，不含17:30整點之後的時間）
	return nowMinutes >= startMinutes && nowMinutes < endMinutes
}

func sendNotify(ctx context.Context, data model.NotifyRequest, url string) error {
	payload := model.NotifyRequest{
		Type:               data.Type,
		DeviceCode:         data.DeviceCode,
		DeviceName:         data.DeviceName,
		ZoneName:           data.ZoneName,
		CurrentTemperature: data.CurrentTemperature,
		Limit:              data.Limit,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("序列化通知內容失敗: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("建立通知請求失敗: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("發送通知失敗: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("通知 API 回傳非預期狀態碼: %d", resp.StatusCode)
	}

	return nil
}

func (h *SensorHook) worker() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case p := <-h.queryCh:
			h.handleQuery(p)
		}
	}
}

func (h *SensorHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	payload := append([]byte(nil), pk.Payload...)
	select {
	case h.queryCh <- payload:
	default:
		log.Printf("[MQTT] queryCh is full, dropping payload: %s", payload)
	}
}

func (h *SensorHook) handleQuery(payload []byte) {
	var sensorData model.SensorData
	if err := json.Unmarshal(payload, &sensorData); err != nil {
		log.Printf("[MQTT] JSON unmarshal error: %v", err)
		return
	}

	if sensorData.DeviceCode == nil || *sensorData.DeviceCode == "" {
		log.Printf("[MQTT] missing device_code, payload: %s", payload)
		return
	}

	ctx, cancel := context.WithTimeout(h.ctx, 30*time.Second)
	defer cancel()

	savedLog, err := h.tempLogRepo.Create(ctx, sensorData)
	if err != nil {
		log.Printf("[DB] insert temperature log failed: %v", err)
		return
	}

	if !IsWithinOfficeHours() {
		// 如果上次更新時間大於一個小時，則更新 TCSDeviceMapping
		h.queryMu.Lock()
		needRefresh := time.Since(lastUpdatedAt) > time.Hour
		if needRefresh {
			lastUpdatedAt = time.Now()
		}
		h.queryMu.Unlock()
		if needRefresh {
			if err := RefreshTCSDeviceMap(ctx, h.tcsDeviceRepo); err != nil {
				log.Printf("[SQL] failed to update TCS devices: %v", err)
			}
		}

		CheckTemperatureLimits(ctx, savedLog, h.tracker)
	}

	Broadcast(*savedLog)
}

func StartMQTTBroker(ctx context.Context, tempLogRepo *repository.TemperatureLogRepository, tcsDeviceRepo *repository.TCSDeviceRepository, tracker *model.ExceedLimitTracker) {
	broker := mqtt.New(&mqtt.Options{
		InlineClient: true,
	})

	if err := broker.AddHook(new(auth.Hook), &auth.Options{
		Ledger: &auth.Ledger{
			Auth: auth.AuthRules{
				{
					Username: auth.RString("iot-test"),
					Password: auth.RString("iot-test"),
					Allow:    true,
				},
				{Allow: false},
			},
			Users: auth.Users{
				"iot-test": auth.UserRule{
					Password: auth.RString("iot-test"),
					ACL: auth.Filters{
						"#": auth.ReadWrite,
					},
				},
			},
		},
	}); err != nil {
		log.Fatalf("[MQTT Broker] AddHook error: %v", err)
	}

	tcpListener := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: ":1883",
	})
	if err := broker.AddListener(tcpListener); err != nil {
		log.Fatalf("[MQTT Broker] AddListener error: %v", err)
	}

	sensorHook := &SensorHook{ctx: ctx, tempLogRepo: tempLogRepo, tcsDeviceRepo: tcsDeviceRepo, tracker: tracker, queryCh: make(chan []byte, 1000)}
	go sensorHook.worker()
	if err := broker.AddHook(sensorHook, nil); err != nil {
		log.Fatalf("[MQTT Broker] AddHook error: %v", err)
	}

	go func() {
		log.Println("[MQTT Broker] Listening on :1883")
		if err := broker.Serve(); err != nil {
			log.Fatalf("[MQTT Broker] Serve error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		log.Println("[MQTT Broker] shutting down...")
		if err := broker.Close(); err != nil {
			log.Printf("[MQTT Broker] Close error: %v", err)
		}
	}()
}
