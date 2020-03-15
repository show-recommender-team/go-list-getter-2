package utils

import (
	"fmt"

	"github.com/spf13/viper"
)

func GetConfig() {
	viper.SetDefault("bucket", "show-data-lake")
	viper.SetDefault("region", "us-east-1")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}
