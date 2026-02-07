package michaelcirkl.ubsa;

public class BlobStorageClientFactory {
    private BlobStorageClientFactory() {
    }

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
        AWS("software.amazon.awssdk.services.s3.S3AsyncClient", "michaelcirkl.ubsa.impl.async.AWSAsyncClientImpl"),
        AZURE("com.azure.storage.blob.BlobServiceAsyncClient", "michaelcirkl.ubsa.impl.async.AzureAsyncClientImpl"),
        GCP("com.google.cloud.storage.Storage", "michaelcirkl.ubsa.impl.async.GCPAsyncClientImpl");

        private final String sdkClassName;
        private final String implClassName;

        AsyncAdapter(String sdkClassName, String implClassName) {
            this.sdkClassName = sdkClassName;
            this.implClassName = implClassName;
        }
    }

    private enum SyncAdapter {
        AWS("software.amazon.awssdk.services.s3.S3Client", "michaelcirkl.ubsa.impl.sync.AWSSyncClientImpl"),
        AZURE("com.azure.storage.blob.BlobServiceClient", "michaelcirkl.ubsa.impl.sync.AzureSyncClientImpl"),
        GCP("com.google.cloud.storage.Storage", "michaelcirkl.ubsa.impl.sync.GCPSyncClientImpl");

        private final String sdkClassName;
        private final String implClassName;

        SyncAdapter(String sdkClassName, String implClassName) {
            this.sdkClassName = sdkClassName;
            this.implClassName = implClassName;
        }
    }

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
