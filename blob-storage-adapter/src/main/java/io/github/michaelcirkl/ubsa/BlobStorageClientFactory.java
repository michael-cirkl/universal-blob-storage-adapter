package io.github.michaelcirkl.ubsa;

/**
 * Factory for adapting provider SDK clients to the UBSA sync or async interfaces.
 *
 * <p>Supported inputs are AWS S3, Azure Blob Storage, and Google Cloud Storage SDK clients. The
 * concrete SDK client type determines which UBSA adapter implementation is created.
 */
public class BlobStorageClientFactory {
    private BlobStorageClientFactory() {
    }

    /**
     * Wraps a supported provider SDK client in a {@link BlobStorageAsyncClient}.
     *
     * <p>Supported client types are AWS {@code S3AsyncClient}, Azure {@code BlobServiceAsyncClient},
     * and GCP {@code Storage}.
     *
     * @param client the native provider SDK client to adapt
     * @return a UBSA async client backed by the given SDK client
     * @throws IllegalArgumentException when the client type is not supported
     */
    public static BlobStorageAsyncClient getAsyncClient(Object client) {
        for (AsyncAdapter adapter : AsyncAdapter.values()) {
            BlobStorageAsyncClient adapted = adaptClient(
                    client,
                    adapter.sdkClassName,
                    adapter.implClassName,
                    BlobStorageAsyncClient.class
            );
            if (adapted != null) {
                return adapted;
            }
        }

        throw new IllegalArgumentException("Unsupported client type: " + client.getClass().getName() + ". Pass in either S3 S3AsyncClient, Azure BlobServiceAsyncClient or GCP Storage");
    }

    /**
     * Wraps a supported provider SDK client in a {@link BlobStorageSyncClient}.
     *
     * <p>Supported client types are AWS {@code S3Client}, Azure {@code BlobServiceClient}, and GCP
     * {@code Storage}.
     *
     * @param client the native provider SDK client to adapt
     * @return a UBSA sync client backed by the given SDK client
     * @throws IllegalArgumentException when the client type is not supported
     */
    public static BlobStorageSyncClient getSyncClient(Object client) {
        for (SyncAdapter adapter : SyncAdapter.values()) {
            BlobStorageSyncClient adapted = adaptClient(
                    client,
                    adapter.sdkClassName,
                    adapter.implClassName,
                    BlobStorageSyncClient.class
            );
            if (adapted != null) {
                return adapted;
            }
        }

        throw new IllegalArgumentException("Unsupported client type: " + client.getClass().getName() + ". Pass in either S3 S3Client, Azure BlobServiceClient or GCP Storage");
    }

    private enum AsyncAdapter {
        AWS("software.amazon.awssdk.services.s3.S3AsyncClient", "io.github.michaelcirkl.ubsa.client.aws.AWSAsyncClientImpl"),
        AZURE("com.azure.storage.blob.BlobServiceAsyncClient", "io.github.michaelcirkl.ubsa.client.azure.AzureAsyncClientImpl"),
        GCP("com.google.cloud.storage.Storage", "io.github.michaelcirkl.ubsa.client.gcp.GCPAsyncClientImpl");

        private final String sdkClassName;
        private final String implClassName;

        AsyncAdapter(String sdkClassName, String implClassName) {
            this.sdkClassName = sdkClassName;
            this.implClassName = implClassName;
        }
    }

    private enum SyncAdapter {
        AWS("software.amazon.awssdk.services.s3.S3Client", "io.github.michaelcirkl.ubsa.client.aws.AWSSyncClientImpl"),
        AZURE("com.azure.storage.blob.BlobServiceClient", "io.github.michaelcirkl.ubsa.client.azure.AzureSyncClientImpl"),
        GCP("com.google.cloud.storage.Storage", "io.github.michaelcirkl.ubsa.client.gcp.GCPSyncClientImpl");

        private final String sdkClassName;
        private final String implClassName;

        SyncAdapter(String sdkClassName, String implClassName) {
            this.sdkClassName = sdkClassName;
            this.implClassName = implClassName;
        }
    }

    // Document using terms used in section client wrapping
    private static <T> T adaptClient(
            Object client,
            String sdkClassName,
            String implClassName,
            Class<T> expectedType
    ) {
        // Optional SDK dependency: if the SDK isn't on the classpath, this adapter is not applicable.
        Class<?> sdkClass;
        try {
            sdkClass = Class.forName(sdkClassName);
        } catch (ClassNotFoundException ignored) {
            return null;
        }

        // Fast exit: the provided client is not of the expected SDK type.
        if (!sdkClass.isInstance(client)) {
            return null;
        }

        // Instantiate the adapter implementation that wraps the SDK client.
        try {
            Class<?> implClass = Class.forName(implClassName);
            Object instance = implClass.getConstructor(sdkClass).newInstance(client);
            return expectedType.cast(instance);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create client adapter for " + sdkClassName, e);
        }
    }
}
