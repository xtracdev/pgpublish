package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/envinject"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgpublish"
)

func main() {
	env, err := envinject.NewInjectedEnv()
	if err != nil {
		log.Fatal(err.Error())
	}

	pgdb, err := pgconn.OpenAndConnect(env, 3)
	if err != nil {
		log.Fatal(err.Error())
	}

	topicARN := env.Getenv(pgpublish.TopicARN)
	if topicARN == "" {
		log.Fatalf("%s not specified in the environment", pgpublish.TopicARN)
	}

	if err = pgpublish.SetLogLevel(pgpublish.LogLevel, env); err != nil {
		log.Warn(err.Error())
	}

	publishEvents := env.Getenv(pgpublish.PublishEnabled)

	publisher, err := pgpublish.NewEvents2Pub(pgdb.DB, topicARN, publishEvents == "1")
	if err != nil {
		log.Fatal(err.Error())
	}

	publisher.InitMetricsSink()
	for {
		pgpublish.PublishEvents(publisher)
	}

}
