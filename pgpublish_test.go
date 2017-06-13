package pgpublish

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSetLogLevel(t *testing.T) {
	testCases := []struct {
		logLevel string
		err      error
	}{
		{"debug", nil},
		{"dEbUG", nil},
		{"Info", nil},
		{"WARN", nil},
		{"ErroR", nil},
		{"Fatal", errors.New("Fatal")},
		{"NoGo", errors.New("NoGo")},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("log level %s", tc.logLevel), func(t *testing.T) {
			os.Setenv(LogLevel, tc.logLevel)
			err := SetLogLevel(LogLevel)
			switch tc.err {
			case nil:
				if err != nil {
					t.Error("Expected nil error")
				}
			default:
				if !strings.Contains(err.Error(), tc.err.Error()) {
					t.Error("Expected log level in error message")
				}
			}
		})

	}

}

func TestDecodeEncoded(t *testing.T) {
	now := time.Now()
	encoded := EncodePGEvent("aggid", 123456, []byte("Now it the time"), "abcde", now)

	a, v, p, t1, t2, err := DecodePGEvent(encoded)
	if assert.Nil(t, err) {
		assert.Equal(t, "aggid", a, "Different aggregate id than expected")
		assert.Equal(t, 123456, v, "Different version id than expected")
		assert.Equal(t, []byte("Now it the time"), p, "Different payload id than expected")
		assert.Equal(t, "abcde", t1, "Different type code than expected")
		assert.True(t, now.Equal(t2), "Different time than expected")

	}
}

func TestPublishStateFromEnv(t *testing.T) {
	publisher, err := NewEvents2Pub(nil, "")
	if assert.Nil(t, err) {
		assert.False(t, publisher.publishingEnabled)
	}

	os.Setenv(PublishEnabled, "1")
	publisher, err = NewEvents2Pub(nil, "")
	if assert.Nil(t, err) {
		assert.True(t, publisher.publishingEnabled)
	}
}
