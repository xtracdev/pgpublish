package db

import (
	. "github.com/gucumber/gucumber"
	"github.com/xtracdev/pgconn"
	"log"
	"os"
	"github.com/xtracdev/pgeventstore"
	"github.com/xtracdev/pgpublish"
	"github.com/xtracdev/goes/sample/testagg"
	"fmt"
	"github.com/stretchr/testify/assert"
)

func init() {

	var eventStore *pgeventstore.PGEventStore
	var publisher *pgpublish.Events2Pub
	var publishedId string
	var e2p []pgpublish.Event2Publish
	var pgdb *pgconn.PostgresDB

	Before("@events2pub", func() {
		var err error
		eventConfig,err := pgconn.NewEnvConfig()
		if err != nil {
			log.Fatal(err.Error())
		}

		pgdb, err = pgconn.OpenAndConnect(eventConfig.ConnectString(), 3)
		if err != nil {
			log.Fatal(err.Error())
		}

		stmt, err := pgdb.DB.Prepare("delete from es.t_aepb_publish")
		if assert.Nil(T, err) {
			defer stmt.Close()
			_, err = stmt.Exec()
			assert.Nil(T, err)
		}

		os.Setenv("ES_PUBLISH_EVENTS", "1")

		eventStore,err = pgeventstore.NewPGEventStore(pgdb.DB)
		if err != nil {
			log.Fatal(err.Error())
		}

		publisher, err = pgpublish.NewEvents2Pub(pgdb.DB, "")
		if err != nil {
			log.Fatal(err.Error())
		}

	})

	Given(`^events for aggregates in the publish table$`, func() {

		aggregate, err := testagg.NewTestAgg(
			fmt.Sprintf("foo-%s", publishedId),
			fmt.Sprintf("foo-%s", publishedId),
			fmt.Sprintf("foo-%s", publishedId))

		publishedId = aggregate.AggregateID

		if err != nil {
			log.Fatal(err)
		}

		err = aggregate.Store(eventStore)
		if err != nil {
			log.Fatal(err)
		}
	})

	When(`^I query for the events$`, func() {
		var err error
		e2p, err = publisher.AggsWithEvents()
		assert.Nil(T, err)
	})

	Then(`^Events2Pub returns the events$`, func() {
		if assert.True(T, len(e2p) == 1) {
			print("========> ", publishedId, " ", e2p[0].AggregateId)
			assert.Equal(T, publishedId, e2p[0].AggregateId)
		}
	})

	After("@events2pub", func() {
		stmt, err := pgdb.DB.Prepare("delete from es.t_aepb_publish where aggregate_id = $1")
		if assert.Nil(T, err) {
			defer stmt.Close()

				_, err = stmt.Exec(publishedId)
				assert.Nil(T, err)
		}
	})
}