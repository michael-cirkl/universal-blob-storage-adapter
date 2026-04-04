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
        S3Client nativeClient = S3Client.builder()
                .region(Region.of(env.required("AWS_REGION")))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        env.required("AWS_ACCESS_KEY_ID"),
                        env.required("AWS_SECRET_ACCESS_KEY")
                )))
                .endpointOverride(URI.create(env.required("AWS_S3_ENDPOINT")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        return new InitializedFixture(nativeClient, BlobStorageClientFactory.getSyncClient(nativeClient));
        });
    }

    private static SyncProviderFixture createAzureFixture() {
        return new SyncProviderFixture(Provider.Azure, () -> {
            TestEnvironment env = TestEnvironment.load();
            String accountName = env.required("AZURE_STORAGE_ACCOUNT_NAME");
            String accountKey = env.required("AZURE_STORAGE_ACCOUNT_KEY");
            String connectionString = "DefaultEndpointsProtocol=http;"
                    + "AccountName=" + accountName + ";"
                    + "AccountKey=" + accountKey + ";"
                    + "BlobEndpoint=http://127.0.0.1:10000/" + accountName + ";";
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
