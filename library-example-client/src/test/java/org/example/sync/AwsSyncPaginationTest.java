package org.example.sync;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.sync.AWSSyncClientImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsSyncPaginationTest {
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
