package db

import (
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/envinject"
	"github.com/xtracdev/goes/sample/testagg"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgeventstore"
	"github.com/xtracdev/pgpublish"
	"log"
	"os"
)

func init() {
	var eventStore *pgeventstore.PGEventStore
	var pgdb *pgconn.PostgresDB
	var publisher *pgpublish.EventStorePublisher
	var publishedId string
	var e2p []pgpublish.Event2Publish

	Before("@snspub", func() {
		var err error
		env, err := envinject.NewInjectedEnv()
		if err != nil {
			log.Fatal(err.Error())
		}

		pgdb, err = pgconn.OpenAndConnect(env, 3)
		if err != nil {
			log.Fatal(err.Error())
		}

		var publishEvents = true

		eventStore, err = pgeventstore.NewPGEventStore(pgdb.DB, publishEvents)
		if err != nil {
			log.Fatal(err.Error())
		}

		topicArn := os.Getenv(pgpublish.TopicARN)

		publisher, err = pgpublish.NewEvents2Pub(pgdb.DB, topicArn, publishEvents)
		if err != nil {
			log.Fatal(err.Error())
		}
	})

	Given(`^a persisted aggregate$`, func() {
		aggregate, err := testagg.NewTestAgg("foo", "bar", "baz")
		if assert.Nil(T, err) {
			publishedId = aggregate.AggregateID
			err := aggregate.Store(eventStore)
			assert.Nil(T, err)
		}
	})

	When(`^I query for events to publish$`, func() {
		var err error
		e2p, err = publisher.AggsWithEvents()
		assert.Nil(T, err)
	})

	Then(`^I obtain all aggs with unpublished events$`, func() {
		if assert.True(T, len(e2p) == 1) {
			assert.Equal(T, publishedId, e2p[0].AggregateId)
		}
	})

	And(`^I can publish all events without error$`, func() {
		err := publisher.PublishEvent(&(e2p[0]))
		assert.Nil(T, err)
	})

}
