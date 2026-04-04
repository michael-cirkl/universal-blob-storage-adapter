package support;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Provider;
import michaelcirkl.ubsa.client.exception.UbsaException;
import org.opentest4j.TestAbortedException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public final class SyncProviderFixture implements AutoCloseable {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    // Creds for emulator
    private static final String AZURE_DEVSTORE_ACCOUNT = "devstoreaccount1";
    private static final String AZURE_DEVSTORE_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private final Provider provider;
    private final FixtureInitializer initializer;
    private InitializedFixture initializedFixture;
    private RuntimeException initializationError;

    private SyncProviderFixture(Provider provider, FixtureInitializer initializer) {
        this.provider = provider;
        this.initializer = initializer;
    }

    public static SyncProviderFixture create(Provider provider) {
        return switch (provider) {
            case AWS -> createAwsFixture();
            case Azure -> createAzureFixture();
            case GCP -> createGcpFixture();
        };
    }

    public Provider provider() {
        return provider;
    }

    public BlobStorageSyncClient client() {
        return fixture().client;
    }

    public SyncTestContext openContext() {
        return new SyncTestContext(this);
    }

    HttpResponse<byte[]> getSignedUrlResponse(URL url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url.toString()))
                .GET()
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    HttpResponse<byte[]> putSignedUrlResponse(URL url, String contentType, byte[] payload) throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url.toString()))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(payload));

        if (contentType != null && !contentType.isBlank()) {
            builder.header("Content-Type", contentType);
        }
        if (provider == Provider.Azure) {
            builder.header("x-ms-blob-type", "BlockBlob");
        }

        return HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    byte[] readRequired(URL url) throws IOException, InterruptedException {
        HttpResponse<byte[]> response = getSignedUrlResponse(url);
        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException("Expected signed GET to return 200 but got " + response.statusCode() + ".");
        }
        return response.body();
    }

    void writeRequired(URL url, String contentType, byte[] payload) throws IOException, InterruptedException {
        HttpResponse<byte[]> response = putSignedUrlResponse(url, contentType, payload);
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            throw new IllegalStateException("Expected signed PUT to succeed but got " + statusCode + ".");
        }
    }

    void cleanupBucket(String bucketName) {
        try {
            BlobStorageSyncClient client = fixture().client;
            for (var blob : client.iterateBlobs(bucketName, null, 100)) {
                client.deleteBlobIfExists(bucketName, blob.getKey());
            }
            client.deleteBucketIfExists(bucketName);
        } catch (UbsaException error) {
            if (error.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                throw error;
            }
        }
    }

    @Override
    public void close() {
        if (initializedFixture == null) {
            return;
        }
        try {
            initializedFixture.nativeClient.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + provider + " test client.", e);
        }
    }

    @Override
    public String toString() {
        return provider.name();
    }

    private static SyncProviderFixture createAwsFixture() {
        return new SyncProviderFixture(Provider.AWS, () -> {
            TestEnvironment env = TestEnvironment.load();
            String region = requiredOrSkip(
                    env,
                    "AWS_REGION",
                    "Skipping AWS tests: configure AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY for a real S3 account."
            );
            String accessKeyId = requiredOrSkip(
                    env,
                    "AWS_ACCESS_KEY_ID",
                    "Skipping AWS tests: configure AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY for a real S3 account."
            );
            String secretAccessKey = requiredOrSkip(
                    env,
                    "AWS_SECRET_ACCESS_KEY",
                    "Skipping AWS tests: configure AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY."
            );
            String endpoint = optional(env, "AWS_S3_ENDPOINT");

            var builder = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            accessKeyId,
                            secretAccessKey
                    )));
            if (endpoint != null) {
                builder.endpointOverride(URI.create(endpoint));
                builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
            }

            S3Client nativeClient = builder.build();
            return new InitializedFixture(nativeClient, BlobStorageClientFactory.getSyncClient(nativeClient));
        });
    }

    private static SyncProviderFixture createAzureFixture() {
        return new SyncProviderFixture(Provider.Azure, () -> {
            TestEnvironment env = TestEnvironment.load();
            String connectionString = azureConnectionString(env);
            BlobServiceClient nativeClient = new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .serviceVersion(BlobServiceVersion.V2025_11_05)
                    .buildClient();
            return new InitializedFixture(() -> { }, BlobStorageClientFactory.getSyncClient(nativeClient));
        });
    }

    private static SyncProviderFixture createGcpFixture() {
        return new SyncProviderFixture(Provider.GCP, () -> {
            TestEnvironment env = TestEnvironment.load();
            String projectId;
            try {
                projectId = env.required("GCP_PROJECT_ID");
            } catch (IllegalStateException error) {
                throw new TestAbortedException(
                        "Skipping GCP tests: no GCP backend configured. Set GCP_PROJECT_ID and configure Application Default Credentials "
                                + "(for example GOOGLE_APPLICATION_CREDENTIALS or `gcloud auth application-default login`).",
                        error
                );
            }

            GoogleCredentials credentials;
            try {
                credentials = GoogleCredentials.getApplicationDefault();
            } catch (IOException error) {
                throw new TestAbortedException(
                        "Skipping GCP tests: Application Default Credentials are not configured. Set GOOGLE_APPLICATION_CREDENTIALS "
                                + "or run `gcloud auth application-default login`.",
                        error
                );
            }

            Storage nativeClient = StorageOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials)
                    .build()
                    .getService();
            return new InitializedFixture(() -> { }, BlobStorageClientFactory.getSyncClient(nativeClient));
        });
    }

    private static String azureConnectionString(TestEnvironment env) {
        String connectionString = optional(env, "AZURE_STORAGE_CONNECTION_STRING");
        if (connectionString != null) {
            return connectionString;
        }

        String accountName = requiredOrSkip(
                env,
                "AZURE_STORAGE_ACCOUNT_NAME",
                "Skipping Azure tests: configure AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY for a real storage account."
        );
        String accountKey = requiredOrSkip(
                env,
                "AZURE_STORAGE_ACCOUNT_KEY",
                "Skipping Azure tests: configure AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY."
        );
        if (AZURE_DEVSTORE_ACCOUNT.equals(accountName) || AZURE_DEVSTORE_KEY.equals(accountKey)) {
            return "DefaultEndpointsProtocol=http;"
                    + "AccountName=" + accountName + ";"
                    + "AccountKey=" + accountKey + ";"
                    + "BlobEndpoint=http://127.0.0.1:10000/" + accountName + ";";
        }
        return "DefaultEndpointsProtocol=https;"
                + "AccountName=" + accountName + ";"
                + "AccountKey=" + accountKey + ";"
                + "EndpointSuffix=core.windows.net";
    }

    private static String requiredOrSkip(TestEnvironment env, String key, String message) {
        try {
            return env.required(key);
        } catch (IllegalStateException error) {
            throw new TestAbortedException(message, error);
        }
    }

    private static String optional(TestEnvironment env, String key) {
        try {
            return env.required(key);
        } catch (IllegalStateException ignored) {
            return null;
        }
    }

    private synchronized InitializedFixture fixture() {
        if (initializedFixture != null) {
            return initializedFixture;
        }
        if (initializationError != null) {
            throw initializationError;
        }
        try {
            initializedFixture = initializer.initialize();
            return initializedFixture;
        } catch (RuntimeException error) {
            initializationError = error;
            throw error;
        }
    }

    @FunctionalInterface
    private interface FixtureInitializer {
        InitializedFixture initialize();
    }

    private static final class InitializedFixture {
        private final AutoCloseable nativeClient;
        private final BlobStorageSyncClient client;

        private InitializedFixture(AutoCloseable nativeClient, BlobStorageSyncClient client) {
            this.nativeClient = nativeClient;
            this.client = client;
        }
    }
}
