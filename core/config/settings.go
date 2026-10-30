package config

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type PostgresConfig struct {
	Host     string `koanf:"host" mapstructure:"host"`
	Port     int    `koanf:"port" mapstructure:"port"`
	User     string `koanf:"user" mapstructure:"user"`
	Password string `koanf:"password" mapstructure:"password"`
	Database string `koanf:"database" mapstructure:"database"`
	SSLMode  string `koanf:"sslmode" mapstructure:"sslmode"`
	MaxPool  int32  `koanf:"max_pool" mapstructure:"max_pool"`
}
type AppConfig struct {
	Mode         string         `koanf:"mode" mapstructure:"mode"`
	Version      string         `koanf:"version" mapstructure:"version"`
	TimeTimezone string         `koanf:"time_timezone" mapstructure:"time_timezone"`
	HTTPAddr     string         `koanf:"http_addr" mapstructure:"http_addr"`
	Location     *time.Location `koanf:"-" mapstructure:"-" json:"-"`
}

type APIConfig struct {
	BearerToken string `koanf:"bearer_token" mapstructure:"bearer_token" json:"-"`
}

type ServiceConfig struct {
	App      AppConfig      `koanf:"app" mapstructure:"app"`
	API      APIConfig      `koanf:"api" mapstructure:"api"`
	Postgres PostgresConfig `koanf:"postgres" mapstructure:"postgres"`
}

func (s *ServiceConfig) Validate() error {
	if s.App.TimeTimezone == "" {
		s.App.TimeTimezone = "UTC"
	}
	if s.App.HTTPAddr == "" {
		s.App.HTTPAddr = ":8080"
	}
	if strings.TrimSpace(s.API.BearerToken) == "" {
		return fmt.Errorf("api.bearer_token is required")
	}
	loc, err := time.LoadLocation(s.App.TimeTimezone)
	if err != nil {
		return fmt.Errorf("invalid timezone '%s': %w", s.App.TimeTimezone, err)
	}
	s.App.Location = loc
	return nil
}
func (s *ServiceConfig) ReadConfig(pathYaml string) {
	k := koanf.New(".")
	// 1) YAML (база)
	if pathYaml != "" {
		if err := k.Load(file.Provider(pathYaml), yaml.Parser()); err != nil {
			log.Fatalf("YAML ERROR: %v", err)
		}
	}
	// 2) ENV (оверрайд поверх YAML)
	// Используем "__" как разделитель вложенности:
	// BUSYBEE_POSTGRES__HOST -> postgres.host
	// BUSYBEE_APP__TIME_TIMEZONE -> app.time_timezone
	// Одиночный "_" остаётся частью имени ключа (max_pool, time_timezone).
	if err := k.Load(env.Provider("BUSYBEE_", ".", func(key string) string {
		key = strings.TrimPrefix(key, "BUSYBEE_")
		key = strings.ToLower(key)
		return strings.ReplaceAll(key, "__", ".")
	}), nil); err != nil {
		log.Fatalf("ENV ERROR: %v", err)
	}
	if err := k.Unmarshal("", s); err != nil {
		log.Fatalf("CONFIG ERROR: cannot unmarshal config: %v", err)
	}
	if s.App.Mode == "development" {
		fmt.Println("=== Current Configuration ===")
		s.PrintConfigJSON()
		fmt.Println("=============================")
	}
}
func (s *ServiceConfig) PrintConfigJSON() {
	jsonData, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling config: %v\n", err)
		return
	}
	fmt.Println(string(jsonData))
}
