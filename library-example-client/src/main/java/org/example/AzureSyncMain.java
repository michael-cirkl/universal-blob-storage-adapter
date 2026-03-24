package org.example;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.Blob;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class AzureSyncMain {
    public static void main(String[] args) throws Exception {
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
        Path uploadFile = null;

        try {
            client.createBucket(Bucket.builder().name(bucketName).build());
            System.out.println("Bucket exists: " + client.bucketExists(bucketName));

            uploadFile = Files.createTempFile("ubsa-azure-sync-", ".txt");
            Files.writeString(uploadFile, "hello from Azure sync client", StandardCharsets.UTF_8);

            String etag = client.createBlob(bucketName, blobKey, uploadFile);
            System.out.println("Created blob etag: " + etag);
            System.out.println("Blob exists: " + client.blobExists(bucketName, blobKey));

            Blob metadata = client.getBlobMetadata(bucketName, blobKey);
            System.out.println("Blob metadata size: " + metadata.getSize());
            System.out.println("Blob metadata content loaded: " + (metadata.getContent() != null));

            byte[] content = client.getBlob(bucketName, blobKey).getContent();
            System.out.println("Blob content: " + new String(content, StandardCharsets.UTF_8));

            ListingPage<Blob> firstBlobPage = client.listBlobs(
                    bucketName,
                    null,
                    PageRequest.builder().pageSize(10).build()
            );
            System.out.println("First blob page size: " + firstBlobPage.getItems().size());
            System.out.println("Has next blob page: " + firstBlobPage.hasNextPage());

            for (Blob listedBlob : client.iterateBlobs(bucketName, null, 10)) {
                System.out.println("Iterated blob key: " + listedBlob.getKey());
            }

            URL putUrl = client.generatePutUrl(bucketName, blobKey, Duration.ofMinutes(10), "text/plain");
            URL getUrl = client.generateGetUrl(bucketName, blobKey, Duration.ofMinutes(10));
            System.out.println("SAS PUT URL: " + putUrl);
            System.out.println("SAS GET URL: " + getUrl);
        } finally {
            try {
                client.deleteBlobIfExists(bucketName, blobKey);
            } catch (Exception cleanupError) {
                System.err.println("Cleanup warning: " + cleanupError.getMessage());
            }
            try {
                client.deleteBucketIfExists(bucketName);
            } catch (Exception cleanupError) {
                System.err.println("Cleanup warning: " + cleanupError.getMessage());
            }
            try {
                if (uploadFile != null) {
                    Files.deleteIfExists(uploadFile);
                }
            } catch (Exception cleanupError) {
                System.err.println("Cleanup warning: " + cleanupError.getMessage());
            }
        }
    }
}
