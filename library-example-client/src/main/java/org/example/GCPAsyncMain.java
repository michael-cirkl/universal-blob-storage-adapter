package org.example;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.Bucket;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class GCPAsyncMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String emulatorHost = System.getenv().getOrDefault(
                "GCP_EMULATOR_ENDPOINT",
                System.getenv().getOrDefault("STORAGE_EMULATOR_HOST", "http://localhost:9023")
        );
        String projectId = System.getenv().getOrDefault("GCP_PROJECT_ID", "demo-project");

        String suffix = String.valueOf(ThreadLocalRandom.current().nextInt(100000, 999999));
        String bucketName = "my-gcp-bucket-" + suffix;
        String blobKey = "example.txt";

        StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder()
                .setProjectId(projectId);
        if (emulatorHost != null && !emulatorHost.isBlank()) {
            optionsBuilder
                    .setHost(emulatorHost)
                    .setCredentials(NoCredentials.getInstance());
        }
        Storage gcpClient = optionsBuilder.build().getService();

        BlobStorageAsyncClient client = BlobStorageClientFactory.getAsyncClient(gcpClient);
        System.out.println("Running provider: " + client.getProvider());

        try {
            client.createBucket(Bucket.builder().name(bucketName).build()).get();
            System.out.println("Bucket exists: " + client.bucketExists(bucketName).get());

            Blob blob = Blob.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .content("hello from GCP async client".getBytes(StandardCharsets.UTF_8))
                    .build();
            String etag = client.createBlob(bucketName, blob).get();
            System.out.println("Created blob etag: " + etag);
            System.out.println("Blob exists: " + client.blobExists(bucketName, blobKey).get());

            byte[] content = client.getBlob(bucketName, blobKey).get().getContent();
            System.out.println("Blob content: " + new String(content, StandardCharsets.UTF_8));

            try {
                URL putUrl = client.generatePutUrl(bucketName, blobKey, Duration.ofMinutes(10), "text/plain").get();
                URL getUrl = client.generateGetUrl(bucketName, blobKey, Duration.ofMinutes(10)).get();
                System.out.println("Signed PUT URL: " + putUrl);
                System.out.println("Signed GET URL: " + getUrl);
            } catch (Exception signingError) {
                System.err.println("Signing warning: " + signingError.getMessage());
            }
        } finally {
            try {
                client.deleteBlob(bucketName, blobKey).get();
            } catch (Exception cleanupError) {
                System.err.println("Cleanup warning: " + cleanupError.getMessage());
            }
            try {
                client.deleteBucketIfExists(bucketName).get();
            } catch (Exception cleanupError) {
                System.err.println("Cleanup warning: " + cleanupError.getMessage());
            }
        }
    }
}
