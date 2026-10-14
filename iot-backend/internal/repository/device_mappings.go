package repository

import (
	"context"
	"fmt"

	"iot-backend/internal/model"

	"gorm.io/gorm"
)

type TCSDeviceRepository struct {
	db *gorm.DB
}

func NewTCSDeviceRepository(db *gorm.DB) *TCSDeviceRepository {
	return &TCSDeviceRepository{db: db}
}

func (r *TCSDeviceRepository) GetTCSDevices(ctx context.Context) ([]model.TCSDeviceMapping, error) {
	var devices []model.TCSDeviceMapping

	err := r.db.WithContext(ctx).
		Select(
			"device_mappings.id",
			"device_mappings.device_code",
			"device_mappings.device_name",
			"device_mappings.zone_id",
			"device_zones.name AS zone_name",
			"device_mappings.upper_limit",
			"device_mappings.lower_limit",
			"device_mappings.memo",
		).
		Joins("JOIN device_zones ON device_mappings.zone_id = device_zones.id").
		Find(&devices).Error

	if err != nil {
		fmt.Printf("查詢 TCS 裝置失敗: %v\n", err)
		return nil, fmt.Errorf("查詢 TCS 裝置失敗: %w", err)
	}

	return devices, nil
}
