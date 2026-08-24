package dstore

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ObjectEntry is one object as a listing returned it: its name, plus the attributes the
// service already sent alongside it.
type ObjectEntry struct {
	// Name is relative to the store, exactly like the name Walk yields.
	Name string

	// Size is the size of the object in bytes.
	Size int64

	// LastModified is the time the object was last modified.
	LastModified time.Time
}

// commonWalkAttributes serves the stores whose listing carries no attributes, by walking and
// asking for each object's attributes. It costs one extra request per object and returns
// exactly what a native implementation would.
func commonWalkAttributes(store Store, ctx context.Context, prefix string, f func(entry ObjectEntry) error) error {
	return store.Walk(ctx, prefix, func(filename string) error {
		attributes, err := store.ObjectAttributes(ctx, filename)
		if err != nil {
			return err
		}

		return f(ObjectEntry{
			Name:         filename,
			Size:         attributes.Size,
			LastModified: attributes.LastModified,
		})
	})
}

// commonListFolders serves the stores that cannot list a single folder level, by walking
// everything under prefix and keeping the distinct first segments. It returns the same folders
// a native implementation would, at the cost of the full enumeration.
func commonListFolders(store Store, ctx context.Context, prefix, inclusiveFrom, exclusiveTo string, max int) ([]string, error) {
	prefix = asFolderPrefix(prefix)
	if err := checkFolderRange(prefix, inclusiveFrom, exclusiveTo); err != nil {
		return nil, err
	}

	folders := newLimitedFolders(max)
	if folders.full() {
		return folders.folders, nil
	}

	seen := map[string]bool{}
	err := store.Walk(ctx, prefix, func(filename string) error {
		name, _, isFolder := strings.Cut(strings.TrimPrefix(filename, prefix), "/")
		if !isFolder || name == "" {
			return nil // an object sitting directly in prefix, not a folder
		}

		folder := prefix + name + "/"
		if seen[folder] {
			return nil
		}
		seen[folder] = true

		if !folderInRange(folder, inclusiveFrom, exclusiveTo) {
			return nil
		}

		folders.add(folder)
		if folders.full() {
			return StopIteration
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return folders.folders, nil
}

// folderInRange reports whether a folder falls in [inclusiveFrom, exclusiveTo). A folder ends
// with a "/", which sorts before any other character a sibling name could continue with, so
// comparing the paths directly gives the range the caller meant.
func folderInRange(folder, inclusiveFrom, exclusiveTo string) bool {
	if inclusiveFrom != "" && folder < inclusiveFrom {
		return false
	}
	if exclusiveTo != "" && folder >= exclusiveTo {
		return false
	}
	return true
}

// checkFolderRange rejects bounds that do not belong to the folder being listed, the same
// contract WalkFromTo enforces.
func checkFolderRange(prefix, inclusiveFrom, exclusiveTo string) error {
	if inclusiveFrom != "" && !strings.HasPrefix(inclusiveFrom, prefix) {
		return fmt.Errorf("inclusive from %q must start with prefix %q", inclusiveFrom, prefix)
	}
	if exclusiveTo != "" && !strings.HasPrefix(exclusiveTo, prefix) {
		return fmt.Errorf("exclusive to %q must start with prefix %q", exclusiveTo, prefix)
	}
	return nil
}

// asFolderPrefix normalizes a folder path to the form the listings expect: either empty, or
// ending with a single "/". Every ListFolders implementation runs its prefix through this, so
// that "alpha" and "alpha/" list the same folder rather than one of them matching the sibling
// "alphabet/" or returning "alpha/" itself.
func asFolderPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	return strings.TrimSuffix(prefix, "/") + "/"
}

// limitedFolders is the shared tail of the native ListFolders implementations, keeping them
// honest about max without repeating the bookkeeping.
type limitedFolders struct {
	max     int
	folders []string
}

func newLimitedFolders(max int) *limitedFolders {
	return &limitedFolders{max: max, folders: []string{}}
}

// full reports whether the limit has been reached and the listing may stop. A max of 0 is
// full from the start.
func (l *limitedFolders) full() bool {
	return l.max >= 0 && len(l.folders) >= l.max
}

func (l *limitedFolders) add(folder string) {
	l.folders = append(l.folders, folder)
}
