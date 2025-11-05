package dstore

import (
	"context"
	"strings"
	"testing"
)

func TestSetMetadata(t *testing.T) {
	ctx := context.Background()

	// Test with MemoryStore
	t.Run("MemoryStore", func(t *testing.T) {
		store, err := NewStore("memory://test", "", "", true)
		if err != nil {
			t.Fatalf("Failed to create memory store: %v", err)
		}

		testSetMetadata(t, ctx, store)
	})

	// Test with MockStore
	t.Run("MockStore", func(t *testing.T) {
		store := NewMockStore(nil)
		store.SetOverwrite(true)

		testSetMetadata(t, ctx, store)
	})

	// Test with LocalStore (should return error)
	t.Run("LocalStore", func(t *testing.T) {
		tmpDir := t.TempDir()
		store, err := NewStore(tmpDir, "", "", true)
		if err != nil {
			t.Fatalf("Failed to create local store: %v", err)
		}

		// Create a test file first
		if err := store.WriteObject(ctx, "test.txt", stringReader("test content")); err != nil {
			t.Fatalf("Failed to write test file: %v", err)
		}

		// SetMetadata should fail for LocalStore
		metadata := map[string]string{"key1": "value1"}
		err = store.SetMetadata(ctx, "test.txt", metadata)
		if err == nil {
			t.Error("Expected SetMetadata to fail for LocalStore, but it succeeded")
		}
		if err.Error() != "metadata is not supported by local file system store" {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})
}

func testSetMetadata(t *testing.T, ctx context.Context, store Store) {
	// Create a test file first
	testContent := "test content for metadata"
	if err := store.WriteObject(ctx, "test-file.txt", stringReader(testContent)); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Test setting metadata
	metadata := map[string]string{
		"author":      "test-user",
		"version":     "1.0",
		"description": "test file for metadata testing",
	}

	err := store.SetMetadata(ctx, "test-file.txt", metadata)
	if err != nil {
		t.Fatalf("Failed to set metadata: %v", err)
	}

	// Verify metadata was set correctly
	attrs, err := store.ObjectAttributes(ctx, "test-file.txt")
	if err != nil {
		t.Fatalf("Failed to get object attributes: %v", err)
	}

	if attrs.Metadata == nil {
		t.Fatal("Expected metadata to be set, but it was nil")
	}

	for key, expectedValue := range metadata {
		if actualValue, exists := attrs.Metadata[key]; !exists {
			t.Errorf("Expected metadata key %q to exist", key)
		} else if actualValue != expectedValue {
			t.Errorf("Expected metadata %q to be %q, got %q", key, expectedValue, actualValue)
		}
	}

	// Test updating metadata
	updatedMetadata := map[string]string{
		"author":  "updated-user",
		"version": "2.0",
		"new-key": "new-value",
	}

	err = store.SetMetadata(ctx, "test-file.txt", updatedMetadata)
	if err != nil {
		t.Fatalf("Failed to update metadata: %v", err)
	}

	// Verify updated metadata
	attrs, err = store.ObjectAttributes(ctx, "test-file.txt")
	if err != nil {
		t.Fatalf("Failed to get updated object attributes: %v", err)
	}

	if len(attrs.Metadata) != len(updatedMetadata) {
		t.Errorf("Expected %d metadata entries, got %d", len(updatedMetadata), len(attrs.Metadata))
	}

	for key, expectedValue := range updatedMetadata {
		if actualValue, exists := attrs.Metadata[key]; !exists {
			t.Errorf("Expected updated metadata key %q to exist", key)
		} else if actualValue != expectedValue {
			t.Errorf("Expected updated metadata %q to be %q, got %q", key, expectedValue, actualValue)
		}
	}

	// Test setting metadata for non-existent file
	err = store.SetMetadata(ctx, "non-existent.txt", metadata)
	if err == nil {
		t.Error("Expected SetMetadata to fail for non-existent file, but it succeeded")
	}
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got: %v", err)
	}

	// Test setting empty metadata
	emptyMetadata := map[string]string{}
	err = store.SetMetadata(ctx, "test-file.txt", emptyMetadata)
	if err != nil {
		t.Fatalf("Failed to set empty metadata: %v", err)
	}

	attrs, err = store.ObjectAttributes(ctx, "test-file.txt")
	if err != nil {
		t.Fatalf("Failed to get object attributes after setting empty metadata: %v", err)
	}

	if len(attrs.Metadata) != 0 {
		t.Errorf("Expected empty metadata, got %d entries", len(attrs.Metadata))
	}
}

func stringReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
