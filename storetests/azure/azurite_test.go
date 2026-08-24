package azure

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/streamingfast/dstore/storetests"
	"github.com/stretchr/testify/require"
)

// Azurite, the Azure Blob emulator, served from docker-compose. See the compose file for the
// environment it expects.
var azEmulatorStoreBaseURL = os.Getenv("STORETESTS_AZ_EMULATOR_STORE_URL")

func TestAZStore_Emulator(t *testing.T) {
	if azEmulatorStoreBaseURL == "" {
		t.Skip("Set STORETESTS_AZ_EMULATOR_STORE_URL (and AZURE_STORAGE_ENDPOINT, AZURE_STORAGE_KEY) to run the Azurite tests, see docker-compose.yml")
		return
	}

	ensureEmulatorContainer(t, azEmulatorStoreBaseURL)
	storetests.TestAll(t, createAZStoreFactory(t, azEmulatorStoreBaseURL, "", false))
}

func TestAZStore_Emulator_CompressedZst(t *testing.T) {
	if azEmulatorStoreBaseURL == "" {
		t.Skip("Set STORETESTS_AZ_EMULATOR_STORE_URL (and AZURE_STORAGE_ENDPOINT, AZURE_STORAGE_KEY) to run the Azurite tests, see docker-compose.yml")
		return
	}

	ensureEmulatorContainer(t, azEmulatorStoreBaseURL)
	storetests.TestAll(t, createAZStoreFactory(t, azEmulatorStoreBaseURL, "zstd", false))
}

// ensureEmulatorContainer creates the container the tests write to, which a real deployment
// would already have.
func ensureEmulatorContainer(t *testing.T, storeURL string) {
	t.Helper()

	trimmed := strings.TrimPrefix(storeURL, "az://")
	accountName, containerName, found := strings.Cut(trimmed, ".")
	require.True(t, found, "store URL %q should look like az://account.container", storeURL)
	containerName, _, _ = strings.Cut(containerName, "/")

	credential, err := azblob.NewSharedKeyCredential(accountName, os.Getenv("AZURE_STORAGE_KEY"))
	require.NoError(t, err)

	endpoint := strings.TrimSuffix(os.Getenv("AZURE_STORAGE_ENDPOINT"), "/") + "/" + accountName + "/"
	client, err := azblob.NewClientWithSharedKeyCredential(endpoint, credential, nil)
	require.NoError(t, err)

	if _, err := client.CreateContainer(context.Background(), containerName, nil); err != nil {
		require.Contains(t, err.Error(), "ContainerAlreadyExists", "creating container %q", containerName)
	}
}
