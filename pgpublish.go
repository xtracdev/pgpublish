package pgpublish

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/armon/go-metrics"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	TopicARN            = "TOPIC_ARN"
	LogLevel            = "PG_PUBLISH_LOG_LEVEL"
	MetricsDumpInterval = 1 * time.Minute
)

var (
	ErrDecodingEvent = errors.New("Error Decoding PG Event")
	metricsSink      = metrics.NewInmemSink(MetricsDumpInterval, 2*MetricsDumpInterval)
	signal           = metrics.DefaultInmemSignal(metricsSink)
	locksAcquired    = []string{"locks_acquired"}
	eventsProcessed  = []string{"events_published"}
	errorCounter     = []string{"errors"}
)

type EventStorePublisher struct {
	db           *sql.DB
	tableLockTxn *sql.Tx
	topicARN     string
	svc          *sns.SNS
}

type Event2Publish struct {
	AggregateId string
	Version     int
	Typecode    string
	Payload     []byte
}

func init() {
	metrics.NewGlobal(metrics.DefaultConfig("pgpublish"), metricsSink)
	pid := syscall.Getpid()
	log.Infof("Using %d for signal pid", pid)
	go func() {
		c := time.Tick(MetricsDumpInterval)
		for range c {
			print("SIGNAL...")
			syscall.Kill(pid, metrics.DefaultSignal)
		}
	}()
}

func warnErrorf(format string, args ...interface{}) {
	metricsSink.IncrCounter(errorCounter, 1)
	log.Warnf(format, args)
}

func NewEvents2Pub(db *sql.DB, topicARN string) (*EventStorePublisher, error) {
	var svc *sns.SNS

	if topicARN != "" {
		session, err := session.NewSession()
		if err != nil {
			warnErrorf("NewEvents2Pub: error creating session: %s", err.Error())
			return nil, err
		}

		svc = sns.New(session)
		if err := CheckTopic(svc, topicARN); err != nil {
			warnErrorf("NewEvents2Pub: error validating topic: %s", err.Error())
			return nil, err
		}
	} else {
		log.Warn("WARNING: No topic specified for NewEvents2Pub - this is only valid for certain test scenarios")
	}

	return &EventStorePublisher{
		db:       db,
		topicARN: topicARN,
		svc:      svc,
	}, nil
}

func CheckTopic(svc *sns.SNS, arn string) error {

	log.Info("check topic", arn)

	params := &sns.GetTopicAttributesInput{
		TopicArn: aws.String(arn),
	}

	_, err := svc.GetTopicAttributes(params)
	return err
}

func (e2p *EventStorePublisher) GetTableLock() (bool, error) {
	log.Debug("start txn for table lock")
	txn, err := e2p.db.Begin()
	if err != nil {
		return false, err
	}

	e2p.tableLockTxn = txn

	log.Debug("execute lock table command")
	_, err = txn.Exec("lock table t_aepl_publock in exclusive mode nowait")

	//No error? Lock was obtained
	if err == nil {
		return true, nil
	}

	//If an error was returned, we distinguish the case were could could not obtain
	//the lock from any other error. Here we assume the lock was not obtained because
	//someone else had the lock

	//First, rollback the transaction or we'll leak resources, specifically the
	//connection allocated with the transaction
	txnErr := txn.Rollback()
	if txnErr != nil {
		warnErrorf("Error rolling back lock table txn: %s", txnErr)
	}

	if strings.Contains(err.Error(), "could not obtain lock on relation") {
		return false, nil
	} else {
		return false, err
	}
}

func (e2p *EventStorePublisher) ReleaseTableLock() error {
	log.Debug("release table lock")
	if e2p.tableLockTxn == nil {
		warnErrorf("Warning - releasing lock from object with no txn")
		return nil
	}

	return e2p.tableLockTxn.Rollback()
}

func (e2p *EventStorePublisher) AggsWithEvents() ([]Event2Publish, error) {
	var events2Publish []Event2Publish

	rows, err := e2p.db.Query(`select aggregate_id, version, typecode, payload from t_aepb_publish limit 25`)
	if err != nil {
		warnErrorf(err.Error())
		return nil, err
	}

	var aggregateId string
	var version int
	var typecode string
	var payload []byte

	for rows.Next() {
		rows.Scan(&aggregateId, &version, &typecode, &payload)
		e2p := Event2Publish{
			AggregateId: aggregateId,
			Version:     version,
			Typecode:    typecode,
			Payload:     payload,
		}
		events2Publish = append(events2Publish, e2p)
	}

	err = rows.Err()

	return events2Publish, nil
}

func EncodePGEvent(aggId string, version int, payload []byte, typecode string) string {
	return fmt.Sprintf("%s:%d:%s:%s",
		aggId, version,
		base64.StdEncoding.EncodeToString(payload),
		typecode)
}

func DecodePGEvent(encoded string) (aggId string, version int, payload []byte, typecode string, err error) {
	parts := strings.Split(encoded, ":")
	if len(parts) != 4 {
		err = ErrDecodingEvent
		return
	}

	aggId = parts[0]
	version, err = strconv.Atoi(parts[1])
	if err != nil {
		return
	}

	payload, err = base64.StdEncoding.DecodeString(parts[2])

	typecode = parts[3]

	return
}

func (e2p *EventStorePublisher) publishEvent(e2pub *Event2Publish) error {
	msg := EncodePGEvent(e2pub.AggregateId, e2pub.Version, e2pub.Payload, e2pub.Typecode)

	params := &sns.PublishInput{
		Message:  aws.String(msg),
		Subject:  aws.String("event for aggregate " + e2pub.AggregateId),
		TopicArn: aws.String(e2p.topicARN),
	}
	resp, err := e2p.svc.Publish(params)

	log.Debug(resp)
	return err
}

func (e2p *EventStorePublisher) PublishEvent(e2pub *Event2Publish) error {

	log.Debugf("Start transaction for %s %d", e2pub.AggregateId, e2pub.Version)
	tx, err := e2p.db.Begin()
	if err != nil {
		warnErrorf("Error starting txn: %s", err.Error())
		return err
	}
	defer tx.Rollback()

	err = e2p.publishEvent(e2pub)
	if err != nil {
		warnErrorf("Error publishing event: %s", err.Error())
		return nil
	}

	log.Debug("delete", e2pub.AggregateId, e2pub.Version)
	_, err = tx.Exec(`delete from t_aepb_publish where aggregate_id = $1 and version = $2`, e2pub.AggregateId, e2pub.Version)
	if err != nil {
		warnErrorf("Error deleting event: %s", err.Error())
		return nil
	}

	log.Debug("commit transaction")
	err = tx.Commit()
	if err != nil {
		warnErrorf("Error committing transaction", err.Error())
		return err
	}

	return nil
}

func SetLogLevel() error {
	origLogLevel := os.Getenv(LogLevel)
	if origLogLevel != "" {
		ll := strings.ToLower(origLogLevel)
		switch ll {
		case "debug":
			log.SetLevel(log.DebugLevel)
		case "info":
			log.SetLevel(log.InfoLevel)
		case "warn":
			log.SetLevel(log.WarnLevel)
		case "error":
			log.SetLevel(log.ErrorLevel)
		default:
			return errors.New(
				fmt.Sprintf("Ignoring log level %s - only debug, info, warn, and error are supported", origLogLevel),
			)
		}
	} else {
		log.Println("Default log level used")
	}

	return nil
}

func delay() {
	time.Sleep(5 * time.Second)
}

func unlockDelay() {
	time.Sleep(1 * time.Second)
}

func PublishEvents(publisher *EventStorePublisher) {
	log.Debug("lock table")
	gotLock, err := publisher.GetTableLock()
	if err != nil {
		warnErrorf("Error locking table: %s", err.Error())
		delay()
		return
	}

	metricsSink.IncrCounter(locksAcquired, 1)

	if !gotLock {
		log.Debug("Did not obtain lock... try again in a bit")
		delay()
		return
	}

	defer func() {
		publisher.ReleaseTableLock()
		unlockDelay()
	}()

	log.Debug("get events to publish")
	events2pub, err := publisher.AggsWithEvents()
	if err != nil {
		warnErrorf("Error retrieving events to publish: %s", err.Error())
		delay()
		return
	}

	log.Debug("check events...")
	numberOfEvents := len(events2pub)
	if numberOfEvents == 0 {
		log.Debug("No events to process")
		delay()
		return
	}

	log.Debugf("Processing %d events to publish", numberOfEvents)
	for _, event := range events2pub {
		err := publisher.PublishEvent(&event)
		if err != nil {
			warnErrorf("Error publishing event: %s", err.Error())
		}
		metricsSink.IncrCounter(eventsProcessed, 1)
	}
}
