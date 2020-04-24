package dstore

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3StoreWriteObject(t *testing.T) {
	t.Skip() // need s3 access to test this, do it on your PC

	//https://s3.console.aws.amazon.com/s3/buckets/dfuse-customer-outbox/?region=us-east-2&tab=overview
	base, _ := url.Parse("s3://dfuse-customer-outbox/testing?region=us-east-2")
	s, err := NewS3Store(base, "", "", false)
	require.NoError(t, err)

	content := "hello world"
	err = s.WriteObject(bctx, "temp.txt", bytes.NewReader([]byte(content)))
	assert.NoError(t, err)

	err = s.Walk(bctx, "eosio.token-transfers-01158", "", func(fname string) error {
		fmt.Println("Listed name", fname)
		return nil
	})
	assert.NoError(t, err)

	rd, err := s.OpenObject(bctx, "temp.txt")
	assert.NoError(t, err)
	cnt, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	rd.Close()
	assert.Equal(t, content, string(cnt))

	err = s.DeleteObject(bctx, "temp.txt")
	assert.NoError(t, err)
}
