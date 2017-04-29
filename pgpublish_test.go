package pgpublish

import (
	"testing"
	"errors"
	"os"
	"strings"
	"fmt"
)

func TestSetLogLevel(t *testing.T) {
	testCases := []struct {
		logLevel string
		err error
	}{
		{"debug",nil},
		{"dEbUG", nil},
		{"Info", nil},
		{"WARN", nil},
		{"ErroR", nil},
		{"Fatal", errors.New("Fatal")},
		{"NoGo", errors.New("NoGo")},
	}

	for _,tc := range testCases {
		t.Run(fmt.Sprintf("log level %s", tc.logLevel), func(t *testing.T) {
			os.Setenv(LogLevel, tc.logLevel)
			err := SetLogLevel()
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
