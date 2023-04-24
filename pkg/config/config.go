package config

import (
	"github.com/spf13/viper"
)

type DBConfig struct {
	User     string
	Password string
	Host     string
	Port     int
	Database string
}

type Config struct {
	DB DBConfig
}

func LoadConfig(filename string) (Config, error) {
	var cfg Config

	// Load config from file
	viper.SetConfigFile(filename)
	if err := viper.ReadInConfig(); err != nil {
		return cfg, err
	}

	// Unmarshal config into struct
	if err := viper.Unmarshal(&cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
