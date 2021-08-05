package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGSParse(t *testing.T) {

	tc := func(in string, expBucket, expObject string) {
		t.Helper()
		b, o := gsparse(in)
		assert.Equal(t, expBucket, b)
		assert.Equal(t, expObject, o)
	}
	tc("sdgfdsf", "", "")
	tc("gs://", "", "")
	tc("gs://a", "", "")
	tc("gs://a/", "", "")
	tc("gs://a/b", "a", "b")
	tc("gs://a/b/c", "a", "b/c")
	tc("gs://a/b/", "a", "b")
	tc("gs://a/b/c/", "a", "b/c")

}
