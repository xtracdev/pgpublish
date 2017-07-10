package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/xtracdev/envinject"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/goes/sample/testagg"
	"github.com/xtracdev/pgconn"
	"github.com/xtracdev/pgeventstore"
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

	eventStore, err := pgeventstore.NewPGEventStore(pgdb.DB, true)
	if err != nil {
		log.Fatal(err.Error())
	}

	anID, _ := goes.GenerateID()
	aggregate, err := testagg.NewTestAgg(
		fmt.Sprintf("foo-%s", anID),
		fmt.Sprintf("foo-%s", anID),
		fmt.Sprintf("foo-%s", anID))

	if err != nil {
		log.Fatal(err)
	}

	aggregate.UpdateFoo(fmt.Sprintf("new=foo-%s", anID))

	err = aggregate.Store(eventStore)
	if err != nil {
		log.Fatal(err)
	}

	pgdb.DB.Close()
}
