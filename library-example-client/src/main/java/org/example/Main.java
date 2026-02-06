package org.example;


import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.impl.AWSClientImpl;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        S3AsyncClient s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();

        BlobStorageClient client = new AWSClientImpl(s3);

        String bucketName = "mybucket";

        client.createBucket(Bucket.builder().name(bucketName).build()).get();
        System.out.println(client.bucketExists(bucketName).get());
        Blob blob = Blob.builder()
                .content("hello from AWS".getBytes())
                .bucket(bucketName)
                .key("example")
                .build();
        client.createBlob(bucketName, blob).get();
        System.out.println(client.blobExists(bucketName, "example").get());
        byte[] content = client.getBlob(bucketName, "example").get().getContent();
        System.out.println(new String(content, StandardCharsets.UTF_8));
        client.deleteBlob(bucketName, "example").get();
        System.out.println(false == client.blobExists(bucketName, "example").get());
    }
}
