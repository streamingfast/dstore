package dstore

import "time"

type ObjectAttributes struct {
	// Size is the size of the object in bytes.
	Size int64

	// LastModified is the time the object was last modified.
	LastModified time.Time
}
