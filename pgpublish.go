package pgpublish

import (
	"database/sql"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"errors"
	"fmt"
	"encoding/base64"
	"strings"
	"strconv"
)

const (
	TopicARN = "TOPIC_ARN"
)

var (
	ErrDecodingEvent = errors.New("Error Decoding PG Event")
)

type Events2Pub struct {
	db       *sql.DB
	tableLockTxn *sql.Tx
	topicARN string
	svc      *sns.SNS
}

type Event2Publish struct {
	AggregateId string
	Version     int
	Typecode    string
	Payload     []byte
}

func NewEvents2Pub(db *sql.DB, topicARN string) (*Events2Pub,error) {
	var svc *sns.SNS

	if topicARN != "" {
		session, err := session.NewSession()
		if err != nil {
			log.Warnf("NewEvents2Pub: error creating session: %s", err.Error())
			return nil, err
		}

		svc = sns.New(session)
		if err := CheckTopic(svc,topicARN); err != nil {
			log.Warnf("NewEvents2Pub: error validating topic: %s", err.Error())
			return nil,err
		}
	} else {
		log.Warn("WARNING: No topic specified for NewEvents2Pub - this is only valid for certain test scenarios")
	}

	return &Events2Pub{
		db:       db,
		topicARN: topicARN,
		svc:      svc,
	}, nil
}


func CheckTopic(svc *sns.SNS, arn string) error {

	log.Println("check topic", arn)

	params := &sns.GetTopicAttributesInput{
		TopicArn: aws.String(arn),
	}

	_, err := svc.GetTopicAttributes(params)
	return err
}

func (e2p *Events2Pub) GetTableLock() (bool,error) {
	log.Info("start txn for table lock")
	txn, err := e2p.db.Begin()
	if err != nil {
		return false, err
	}

	e2p.tableLockTxn = txn

	log.Info("execute lock table command")
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
		log.Warnf("Error rolling back lock table txn: %s", txnErr)
	}

	if strings.Contains(err.Error(), "could not obtain lock on relation") {
		return false, nil
	} else {
		return false, err
	}
}

func (e2p *Events2Pub) ReleaseTableLock() error {
	log.Info("release table lock")
	if e2p.tableLockTxn == nil {
		log.Warn("Warning - releasing lock from object with no txn")
		return nil
	}

	return e2p.tableLockTxn.Rollback()
}

func (e2p *Events2Pub) AggsWithEvents() ([]Event2Publish, error) {
	var events2Publish []Event2Publish

	rows, err := e2p.db.Query(`select aggregate_id, version, typecode, payload from t_aepb_publish limit 25`)
	if err != nil {
		log.Fatal(err)
	}

	var aggregateId string
	var version int
	var typecode string
	var payload []byte

	for rows.Next() {
		rows.Scan(&aggregateId,&version,&typecode,&payload)
		e2p := Event2Publish{
			AggregateId:aggregateId,
			Version:version,
			Typecode:typecode,
			Payload:payload,
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

func (e2p *Events2Pub) publishEvent(e2pub *Event2Publish) error {
	msg := EncodePGEvent(e2pub.AggregateId, e2pub.Version, e2pub.Payload, e2pub.Typecode)

	params := &sns.PublishInput{
		Message:  aws.String(msg),
		Subject:  aws.String("event for aggregate " + e2pub.AggregateId),
		TopicArn: aws.String(e2p.topicARN),
	}
	resp, err := e2p.svc.Publish(params)

	log.Println(resp)
	return err
}

func (e2p *Events2Pub) PublishEvent(e2pub *Event2Publish) error {

	log.Infof("Start transaction for %s %d", e2pub.AggregateId, e2pub.Version)
	tx, err := e2p.db.Begin()
	if err != nil {
		log.Warnf("Error starting txn: %s", err.Error())
		return err
	}
	defer tx.Rollback()

	err = e2p.publishEvent(e2pub)
	if err != nil {
		log.Warnf("Error publishing event: %s", err.Error())
		return nil
	}

	log.Println("delete", e2pub.AggregateId, e2pub.Version)
	_, err = tx.Exec(`delete from t_aepb_publish where aggregate_id = $1 and version = $2`, e2pub.AggregateId, e2pub.Version)
	if err != nil {
		log.Warnf("Error deleting event: %s", err.Error())
		return nil
	}

	log.Println("commit transaction")
	err = tx.Commit()
	if err != nil {
		log.Warn("Error committing transaction", err.Error())
		return err
	}

	return nil
}