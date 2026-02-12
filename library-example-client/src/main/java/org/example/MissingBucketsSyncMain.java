package org.example;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class MissingBucketsSyncMain {
    public static void main(String[] args) {
        runAwsMissingBucketScenario();
        //runGcpMissingBucketScenario();
        //runAzureMissingBucketScenario();
    }

    private static void runAwsMissingBucketScenario() {
        URI endpoint = URI.create(System.getenv().getOrDefault("LOCALSTACK_ENDPOINT", "http://localhost:4566"));
        Region region = Region.of(System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
        String accessKey = System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "test");
        String secretKey = System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "test");

        String bucketName = "missing-s3-" + randomSuffix();
        String blobKey = "missing-example.txt";

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(endpoint)
                .region(region)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .build()) {
            BlobStorageSyncClient client = BlobStorageClientFactory.getSyncClient(s3);
            Blob blob = buildBlob(bucketName, blobKey);

            System.out.println("AWS missing bucket test with: " + bucketName);
            System.out.println("bucketExists -> " + client.bucketExists(bucketName));
            client.createBlob(bucketName, blob);
            client.getBlob(bucketName, blobKey);
        }
    }

    private static void runGcpMissingBucketScenario() {
        String emulatorHost = System.getenv().getOrDefault(
                "GCP_EMULATOR_ENDPOINT",
                System.getenv().getOrDefault("STORAGE_EMULATOR_HOST", "http://localhost:9023")
        );
        String projectId = System.getenv().getOrDefault("GCP_PROJECT_ID", "demo-project");

        String bucketName = "missing-gcp-" + randomSuffix();
        String blobKey = "missing-example.txt";

        StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder()
                .setProjectId(projectId);
        if (emulatorHost != null && !emulatorHost.isBlank()) {
            optionsBuilder
                    .setHost(emulatorHost)
                    .setCredentials(NoCredentials.getInstance());
        }
        Storage gcpClient = optionsBuilder.build().getService();

        BlobStorageSyncClient client = BlobStorageClientFactory.getSyncClient(gcpClient);
        Blob blob = buildBlob(bucketName, blobKey);

        System.out.println("GCP missing bucket test with: " + bucketName);
        System.out.println("bucketExists -> " + client.bucketExists(bucketName));
        client.createBlob(bucketName, blob);
        client.getBlob(bucketName, blobKey);
    }

    private static void runAzureMissingBucketScenario() {
        String accountName = System.getenv().getOrDefault("AZURE_ACCOUNT_NAME", "devstoreaccount1");
        String accountKey = System.getenv().getOrDefault(
                "AZURE_ACCOUNT_KEY",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        );
        String endpoint = System.getenv().getOrDefault(
                "AZURITE_ENDPOINT",
                "http://127.0.0.1:10000/" + accountName
        );

        String bucketName = "missing-azure-" + randomSuffix();
        String blobKey = "missing-example.txt";

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceClient azureClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        BlobStorageSyncClient client = BlobStorageClientFactory.getSyncClient(azureClient);
        Blob blob = buildBlob(bucketName, blobKey);

        System.out.println("Azure missing container test with: " + bucketName);
        System.out.println("bucketExists -> " + client.bucketExists(bucketName));
        client.createBlob(bucketName, blob);
        client.getBlob(bucketName, blobKey);
    }

    private static Blob buildBlob(String bucketName, String blobKey) {
        return Blob.builder()
                .bucket(bucketName)
                .key(blobKey)
                .content("expected-to-fail".getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }
}
