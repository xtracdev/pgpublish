package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/pgconn"
	"os"
	"github.com/xtracdev/pgpublish"
	"time"
)

const (
	TopicARN = "TOPIC_ARN"
)

func delay() {
	time.Sleep(5 * time.Second)
}

func main() {
	eventConfig,err := pgconn.NewEnvConfig()
	if err != nil {
		log.Fatal(err.Error())
	}

	pgdb, err := pgconn.OpenAndConnect(eventConfig.ConnectString(), 3)
	if err != nil {
		log.Fatal(err.Error())
	}

	topicARN := os.Getenv(TopicARN)
	if topicARN == "" {
		log.Fatalf("%s not specified in the environment", TopicARN)
	}


	publisher, err := pgpublish.NewEvents2Pub(pgdb.DB, topicARN)
	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		events2pub, err := publisher.AggsWithEvents()
		if err != nil {
			log.Warnf("Error retrieving events to publish: %s", err.Error())
			delay()
			continue
		}

		numberOfEvents := len(events2pub)
		if numberOfEvents == 0 {
			delay()
			continue
		}

		log.Info("Processing %d events to publish", numberOfEvents)
		for _,event := range events2pub {
			err := publisher.PublishEvent(&event)
			if err != nil {
				log.Warn("Error publishing event: %s", err.Error())
			}
		}
	}

}