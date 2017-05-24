package db

import (
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/goes/sample/testagg"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgeventstore"
	"github.com/xtracdev/pgpublish"
	"log"
	"os"
)

func init() {

	var eventStore *pgeventstore.PGEventStore
	var publisher *pgpublish.EventStorePublisher
	var publishedId string
	var e2p []pgpublish.Event2Publish
	var pgdb *pgconn.PostgresDB

	Before("@events2pub", func() {
		var err error
		eventConfig, err := pgconn.NewEnvConfig()
		if err != nil {
			log.Fatal(err.Error())
		}

		pgdb, err = pgconn.OpenAndConnect(eventConfig.ConnectString(), 3)
		if err != nil {
			log.Fatal(err.Error())
		}

		stmt, err := pgdb.DB.Prepare("delete from t_aepb_publish")
		if err != nil {
			log.Fatal(err.Error())
		}


		defer stmt.Close()
		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err.Error())
		}
		

		os.Setenv("ES_PUBLISH_EVENTS", "1")

		eventStore, err = pgeventstore.NewPGEventStore(pgdb.DB)
		if err != nil {
			log.Fatal(err.Error())
		}

		publisher, err = pgpublish.NewEvents2Pub(pgdb.DB, "")
		if err != nil {
			log.Fatal(err.Error())
		}

	})

	Given(`^events for aggregates in the publish table$`, func() {

		aggregate, err := testagg.NewTestAgg("foo", "bar", "baz")
		publishedId = aggregate.AggregateID

		err = aggregate.Store(eventStore)
		assert.Nil(T, err)
	})

	When(`^I query for the events$`, func() {
		var err error
		e2p, err = publisher.AggsWithEvents()
		assert.Nil(T, err)
	})

	Then(`^Events2Pub returns the events$`, func() {
		if assert.True(T, len(e2p) == 1) {
			assert.Equal(T, publishedId, e2p[0].AggregateId)
		}
	})

	After("@events2pub", func() {
		stmt, err := pgdb.DB.Prepare("delete from t_aepb_publish where aggregate_id = $1")
		if assert.Nil(T, err) {
			defer stmt.Close()

			_, err = stmt.Exec(publishedId)
			assert.Nil(T, err)
		}
	})
}
