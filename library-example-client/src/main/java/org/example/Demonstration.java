package org.example;

import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.UbsaException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.net.URI;

public class Demonstration {
    public static void main(String[] args) {
        S3AsyncClient s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();

        BlobStorageAsyncClient asyncClient = BlobStorageClientFactory.getAsyncClient(s3);
        BlobStorageSyncClient syncClient = BlobStorageClientFactory.getSyncClient(s3);

        // Calling operations
        asyncClient.listAllBuckets();

        // Getting client back, with real return type using generics.
        S3AsyncClient s3client = asyncClient.unwrap(S3AsyncClient.class);

        // Custom generic exceptions. Library throws UbsaException, not native S3Exception, StorageException etc.
        var x = UbsaException.class;
    }
}
