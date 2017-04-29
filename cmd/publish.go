package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgpublish"
	"os"
)

func main() {
	eventConfig, err := pgconn.NewEnvConfig()
	if err != nil {
		log.Fatal(err.Error())
	}

	pgdb, err := pgconn.OpenAndConnect(eventConfig.ConnectString(), 3)
	if err != nil {
		log.Fatal(err.Error())
	}

	topicARN := os.Getenv(pgpublish.TopicARN)
	if topicARN == "" {
		log.Fatalf("%s not specified in the environment", pgpublish.TopicARN)
	}

	if err = pgpublish.SetLogLevel(); err != nil {
		log.Warn(err.Error())
	}

	publisher, err := pgpublish.NewEvents2Pub(pgdb.DB, topicARN)
	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		pgpublish.PublishEvents(publisher)
	}

}
