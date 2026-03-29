package org.example;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class ExampleMain {
    private ExampleMain() {
    }

    public static void main(String[] args) {
        S3Client awsClient = createAwsClient();
        BlobServiceClient azureClient = createAzureClient();
        Storage gcpClient = createGcpClient();

        BlobStorageSyncClient client = BlobStorageClientFactory.getSyncClient(awsClient);

        String bucket = "ubsa-example";
        String key = "hello.txt";
        Blob blobToCreate = Blob.builder()
                .key(key)
                .content("hello from the UBSA example".getBytes(StandardCharsets.UTF_8))
                .build();

        if (!client.bucketExists(bucket)) {
            client.createBucket(Bucket.builder().name(bucket).build());
        }

        String etag = client.createBlob(bucket, blobToCreate);
        Blob metadata = client.getBlobMetadata(bucket, key);
        Blob blob = client.getBlob(bucket, key);
        List<String> blobKeys = client.listBlobs(bucket, null, null)
                .getItems()
                .stream()
                .map(Blob::getKey)
                .toList();
        byte[] firstFiveBytes = client.getByteRange(bucket, key, 0, 4);

        System.out.println("provider=" + client.getProvider());
        System.out.println("bucket=" + bucket);
        System.out.println("blob=" + key);
        System.out.println("etag=" + etag);
        System.out.println("size=" + metadata.getSize());
        System.out.println("content=" + new String(blob.getContent(), StandardCharsets.UTF_8));
        System.out.println("firstFiveBytes=" + new String(firstFiveBytes, StandardCharsets.UTF_8));
        System.out.println("listedBlobs=" + blobKeys);

        client.deleteBlobIfExists(bucket, key);
    }

    private static S3Client createAwsClient() {
        return S3Client.builder()
                .region(Region.of("us-east-1"))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .endpointOverride(URI.create("http://localhost:4566"))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    private static BlobServiceClient createAzureClient() {
        String connectionString =
                "DefaultEndpointsProtocol=http;"
                        + "AccountName=devstoreaccount1;"
                        + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                        + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        return new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    private static Storage createGcpClient() {
        return StorageOptions.newBuilder()
                .setProjectId("test-project")
                .setCredentials(NoCredentials.getInstance())
                .setHost("http://localhost:9023")
                .build()
                .getService();
    }
}
