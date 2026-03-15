package org.example;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class MissingBucketsSyncMain {
    public static void main(String[] args) {
        // Uncomment exactly one provider example at a time.
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
            try {
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(blobKey)
                            .build(),
                    RequestBody.fromString("expected-to-fail", StandardCharsets.UTF_8)
            );


                ResponseBytes<?> response = s3.getObjectAsBytes(GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(blobKey)
                        .build());
                System.out.println(response.asUtf8String());
            } catch (S3Exception e) {
                System.out.println(e.awsErrorDetails().errorCode());
                throw e;
            }


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

        try {
            gcpClient.create(
                    com.google.cloud.storage.BlobInfo.newBuilder(BlobId.of(bucketName, blobKey)).build(),
                    "expected-to-fail".getBytes(StandardCharsets.UTF_8)
            );
            gcpClient.get(BlobId.of(bucketName, blobKey)).getContent();
        } catch (StorageException e) {
            System.out.println("Reason in exception:" + e.getReason());
            throw e;
        }

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

        BlobClient blobClient = azureClient
                .getBlobContainerClient(bucketName)
                .getBlobClient(blobKey);

        try {
            blobClient.upload(com.azure.core.util.BinaryData.fromString("expected-to-fail"));
            System.out.println(blobClient.downloadContent().toString());
        } catch (BlobStorageException e) {
            System.out.println(e.getErrorCode().toString());
        }

    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }
}
