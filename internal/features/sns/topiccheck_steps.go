package sns

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/pgpublish"
	"os"
)

func init() {
	var topicArn string
	var checkTopicError error

	Given(`^a valid topic ARN$`, func() {
		topicArn = os.Getenv(pgpublish.TopicARN)
		assert.NotEqual(T, "", topicArn)
	})

	When(`^check it$`, func() {
		session, err := session.NewSession()
		if assert.Nil(T, err) {
			svc := sns.New(session)
			checkTopicError = pgpublish.CheckTopic(svc, topicArn)
		}

	})

	Then(`^I receive no errors$`, func() {
		assert.Nil(T, checkTopicError)
	})

	Given(`^an invalid top ARN$`, func() {
		topicArn = "invalid arn yeah"
	})

	When(`^I check it$`, func() {
		session, err := session.NewSession()
		if assert.Nil(T, err) {
			svc := sns.New(session)
			checkTopicError = pgpublish.CheckTopic(svc, topicArn)
		}
	})

	Then(`^I receive an error$`, func() {
		assert.NotNil(T, checkTopicError)
	})

}
