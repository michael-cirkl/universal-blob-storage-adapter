package org.example;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.Bucket;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
        BlobServiceAsyncClient azureClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildAsyncClient();

        BlobStorageAsyncClient client = BlobStorageClientFactory.getAsyncClient(azureClient);
        System.out.println("Running provider: " + client.getProvider());

        try {
            client.createBucket(Bucket.builder().name(bucketName).build()).get();
            System.out.println("Bucket exists: " + client.bucketExists(bucketName).get());

            Blob blob = Blob.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .content("hello from Azure async client".getBytes(StandardCharsets.UTF_8))
                    .build();
            String etag = client.createBlob(bucketName, blob).get();
            System.out.println("Created blob etag: " + etag);
            System.out.println("Blob exists: " + client.blobExists(bucketName, blobKey).get());

            byte[] content = client.getBlob(bucketName, blobKey).get().getContent();
            System.out.println("Blob content: " + new String(content, StandardCharsets.UTF_8));

            URL putUrl = client.generatePutUrl(bucketName, blobKey, Duration.ofMinutes(10), "text/plain").get();
            URL getUrl = client.generateGetUrl(bucketName, blobKey, Duration.ofMinutes(10)).get();
            System.out.println("SAS PUT URL: " + putUrl);
            System.out.println("SAS GET URL: " + getUrl);
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
