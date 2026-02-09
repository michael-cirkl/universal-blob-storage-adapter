package org.example;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class AzureSyncMain {
    public static void main(String[] args) {
        String accountName = System.getenv().getOrDefault("AZURE_ACCOUNT_NAME", "devstoreaccount1");
        String accountKey = System.getenv().getOrDefault(
                "AZURE_ACCOUNT_KEY",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        );
        String endpoint = System.getenv().getOrDefault(
                "AZURITE_ENDPOINT",
                "http://127.0.0.1:10000/" + accountName
        );

        String suffix = String.valueOf(ThreadLocalRandom.current().nextInt(100000, 999999));
        String bucketName = "my-container-" + suffix;
        String blobKey = "example.txt";

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceClient azureClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        BlobStorageSyncClient client = BlobStorageClientFactory.getSyncClient(azureClient);
        System.out.println("Running provider: " + client.getProvider());

        try {
            client.createBucket(Bucket.builder().name(bucketName).build());
            System.out.println("Bucket exists: " + client.bucketExists(bucketName));

            Blob blob = Blob.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .content("hello from Azure sync client".getBytes(StandardCharsets.UTF_8))
                    .build();
            String etag = client.createBlob(bucketName, blob);
            System.out.println("Created blob etag: " + etag);
            System.out.println("Blob exists: " + client.blobExists(bucketName, blobKey));

            byte[] content = client.getBlob(bucketName, blobKey).getContent();
            System.out.println("Blob content: " + new String(content, StandardCharsets.UTF_8));

            URL putUrl = client.generatePutUrl(bucketName, blobKey, Duration.ofMinutes(10), "text/plain");
            URL getUrl = client.generateGetUrl(bucketName, blobKey, Duration.ofMinutes(10));
            System.out.println("SAS PUT URL: " + putUrl);
            System.out.println("SAS GET URL: " + getUrl);
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
