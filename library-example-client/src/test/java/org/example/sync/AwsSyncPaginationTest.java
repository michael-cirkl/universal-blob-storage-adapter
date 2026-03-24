package org.example.sync;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.aws.AWSSyncClientImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsSyncPaginationTest {
    @Test
    void listBucketsUsesCreationDateAndLeavesLastModifiedUnset() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);

        ListBucketsResponse response = ListBucketsResponse.builder()
                .continuationToken("page-2")
                .buckets(software.amazon.awssdk.services.s3.model.Bucket.builder()
                        .name("bucket")
                        .creationDate(Instant.parse("2025-01-02T03:04:05Z"))
                        .build())
                .build();

        when(client.listBuckets(any(ListBucketsRequest.class))).thenAnswer(invocation -> {
            ListBucketsRequest request = invocation.getArgument(0);
            assertEquals(5, request.maxBuckets());
            return response;
        });

        ListingPage<michaelcirkl.ubsa.Bucket> buckets = adapter.listBuckets(PageRequest.builder().pageSize(5).build());

        assertEquals(1, buckets.getItems().size());
        michaelcirkl.ubsa.Bucket bucket = buckets.getItems().get(0);
        assertEquals("bucket", bucket.getName());
        assertEquals(java.time.LocalDateTime.of(2025, 1, 2, 3, 4, 5), bucket.getCreationDate());
        assertNull(bucket.getLastModified());
        assertTrue(buckets.hasNextPage());
        assertEquals("page-2", buckets.getNextContinuationToken());
    }

    @Test
    void listBlobsReturnsSinglePageAndContinuationToken() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);

        ListObjectsV2Response firstPage = ListObjectsV2Response.builder()
                .isTruncated(true)
                .nextContinuationToken("page-2")
                .contents(S3Object.builder().key("prefix/blob-1").size(1L).build())
                .build();
        ListObjectsV2Response secondPage = ListObjectsV2Response.builder()
                .isTruncated(false)
                .contents(S3Object.builder().key("prefix/blob-2").size(2L).build())
                .build();

        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(invocation -> {
                    ListObjectsV2Request request = invocation.getArgument(0);
                    assertEquals("prefix/", request.prefix());
                    assertEquals(1, request.maxKeys());
                    return request.continuationToken() == null ? firstPage : secondPage;
                });

        ListingPage<Blob> blobs = adapter.listBlobs("bucket", "prefix/", PageRequest.builder().pageSize(1).build());

        assertEquals(1, blobs.getItems().size());
        assertEquals(List.of("prefix/blob-1"), blobs.getItems().stream().map(Blob::getKey).toList());
        assertTrue(blobs.hasNextPage());
        assertEquals("page-2", blobs.getNextContinuationToken());

        ListingPage<Blob> secondResult = adapter.listBlobs(
                "bucket",
                "prefix/",
                PageRequest.builder().pageSize(1).continuationToken("page-2").build()
        );

        assertEquals(List.of("prefix/blob-2"), secondResult.getItems().stream().map(Blob::getKey).toList());
        assertFalse(secondResult.hasNextPage());
        assertEquals(null, secondResult.getNextContinuationToken());
    }

    @Test
    void listBlobsPreservesDuplicateEntriesWithinPage() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .isTruncated(false)
                .contents(
                        S3Object.builder().key("prefix/blob-1").size(1L).build(),
                        S3Object.builder().key("prefix/blob-1").size(1L).build()
                )
                .build();

        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        ListingPage<Blob> blobs = adapter.listBlobs("bucket", "prefix/", PageRequest.firstPage());

        assertEquals(2, blobs.getItems().size());
        assertEquals(List.of("prefix/blob-1", "prefix/blob-1"), blobs.getItems().stream().map(Blob::getKey).toList());
        assertFalse(blobs.hasNextPage());
    }

    @Test
    void iterateBlobsTraversesMultiplePagesWithForLoop() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);

        ListObjectsV2Response firstPage = ListObjectsV2Response.builder()
                .isTruncated(true)
                .nextContinuationToken("page-2")
                .contents(S3Object.builder().key("prefix/blob-1").size(1L).build())
                .build();
        ListObjectsV2Response secondPage = ListObjectsV2Response.builder()
                .isTruncated(false)
                .contents(S3Object.builder().key("prefix/blob-2").size(2L).build())
                .build();

        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(invocation -> {
                    ListObjectsV2Request request = invocation.getArgument(0);
                    return request.continuationToken() == null ? firstPage : secondPage;
                });

        List<String> keys = new java.util.ArrayList<>();
        for (Blob blob : adapter.iterateBlobs("bucket", "prefix/", 1)) {
            keys.add(blob.getKey());
        }

        assertEquals(List.of("prefix/blob-1", "prefix/blob-2"), keys);
    }

    @Test
    void iterateBlobsRejectsInvalidPageSize() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);

        IllegalArgumentException error = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> adapter.iterateBlobs("bucket", "prefix/", 0)
        );

        assertEquals("Page size must be greater than 0.", error.getMessage());
    }
}
