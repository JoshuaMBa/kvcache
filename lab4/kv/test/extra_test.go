package kvtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

func TestIntegrationClientTtl(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	err := setup.Set("abc", "123", 500*time.Millisecond)
	assert.Nil(t, err)
	assert.Equal(t, 1, setup.clientPool.GetRequestsSent("n1"))

	_, wasFound, _ := setup.Get("abc")
	assert.True(t, wasFound)

	time.Sleep(800 * time.Millisecond)
	assert.Equal(t, 2, setup.clientPool.GetRequestsSent("n1"))

	_, wasFound, _ = setup.Get("abc")
	assert.False(t, wasFound)
	assert.Equal(t, 3, setup.clientPool.GetRequestsSent("n1"))

	setup.Shutdown()
}
