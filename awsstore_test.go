package dstore

import (
	"bytes"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3SToreWriteObject(t *testing.T) {
	t.Skip() // need s3 access to test this, do it on your PC
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "region")

	//https://s3.console.aws.amazon.com/s3/buckets/aws-store-dev-bucket/?region=us-east-2&tab=overviewhttps://s3.console.aws.amazon.com/s3/buckets/aws-store-dev-bucket/?region=us-east-2&tab=overview
	base, _ := url.Parse("s3://aws-store-dev-bucket/test?region=us-east-2")
	s, err := NewS3Store(base, "", "", false)
	assert.NoError(t, err)

	content := "hello world"
	err = s.WriteObject(bctx, "temp.txt", bytes.NewReader([]byte(content)))
	assert.NoError(t, err)

}
