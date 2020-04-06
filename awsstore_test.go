package dstore

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3SToreWriteObject(t *testing.T) {
	t.Skip() // need s3 access to test this, do it on your PC
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "region")

	//https://s3.console.aws.amazon.com/s3/buckets/aws-store-dev-bucket/?region=us-east-2&tab=overviewhttps://s3.console.aws.amazon.com/s3/buckets/aws-store-dev-bucket/?region=us-east-2&tab=overview
	s, err := NewS3Store("aws-store-dev-bucket", "us-east-2", "", "", false)
	assert.NoError(t, err)

	content := "hello world"
	err = s.WriteObject("temp.txt", bytes.NewReader([]byte(content)))
	assert.NoError(t, err)

}
