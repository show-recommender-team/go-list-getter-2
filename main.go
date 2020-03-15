package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/show-recommender-team/go-list-getter-2/etl"
	"github.com/show-recommender-team/go-list-getter-2/utils"
	"github.com/spf13/viper"
)

func main() {
	utils.GetConfig()
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(viper.GetString("region")),
	})
	if err != nil {
		panic(fmt.Sprintf("Couldn't build session: %+v", err))
	}
	bucket := aws.String(viper.GetString("bucket"))
	runner := etl.NewRunner(bucket, sess, time.NewTicker(60*time.Second))
	runner.Do()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	close(runner.Quit)
}
