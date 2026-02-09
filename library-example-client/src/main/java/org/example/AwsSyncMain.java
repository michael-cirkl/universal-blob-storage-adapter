package org.example;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class AwsSyncMain {
    public static void main(String[] args) {
        URI endpoint = URI.create(System.getenv().getOrDefault("LOCALSTACK_ENDPOINT", "http://localhost:4566"));
        Region region = Region.of(System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
        String accessKey = System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "test");
        String secretKey = System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "test");

        String suffix = String.valueOf(ThreadLocalRandom.current().nextInt(100000, 999999));
        String bucketName = "mybucket-" + suffix;
        String blobKey = "example.txt";

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(endpoint)
                .region(region)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .build()) {
            BlobStorageSyncClient client = BlobStorageClientFactory.getSyncClient(s3);
            System.out.println("Running provider: " + client.getProvider());

            try {
                client.createBucket(Bucket.builder().name(bucketName).build());
                System.out.println("Bucket exists: " + client.bucketExists(bucketName));

                Blob blob = Blob.builder()
                        .bucket(bucketName)
                        .key(blobKey)
                        .content("hello from AWS sync client".getBytes(StandardCharsets.UTF_8))
                        .build();
                String etag = client.createBlob(bucketName, blob);
                System.out.println("Created blob etag: " + etag);
                System.out.println("Blob exists: " + client.blobExists(bucketName, blobKey));

                byte[] content = client.getBlob(bucketName, blobKey).getContent();
                System.out.println("Blob content: " + new String(content, StandardCharsets.UTF_8));

                URL putUrl = client.generatePutUrl(bucketName, blobKey, Duration.ofMinutes(10), "text/plain");
                URL getUrl = client.generateGetUrl(bucketName, blobKey, Duration.ofMinutes(10));
                System.out.println("Presigned PUT URL: " + putUrl);
                System.out.println("Presigned GET URL: " + getUrl);
            } finally {
                try {
                    client.deleteBlob(bucketName, blobKey);
                } catch (Exception cleanupError) {
                    System.err.println("Cleanup warning: " + cleanupError.getMessage());
                }
                try {
                    client.deleteBucketIfExists(bucketName);
                } catch (Exception cleanupError) {
                    System.err.println("Cleanup warning: " + cleanupError.getMessage());
                }
            }
        }
    }
}
