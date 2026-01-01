package org.example;


import michaelcirkl.ubsa.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        BlobStorageClientBuilder clientBuilder = BuilderFactory.getClientBuilder();
        BlobStorageClient client = clientBuilder
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .credentials("test", "test")
                .build();

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
