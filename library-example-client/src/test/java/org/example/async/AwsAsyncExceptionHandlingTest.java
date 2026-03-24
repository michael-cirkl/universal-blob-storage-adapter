package org.example.async;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.aws.AWSAsyncClientImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AwsAsyncExceptionHandlingTest {
    @Test
    void listBucketsUsesCreationDateAndLeavesLastModifiedUnset() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);

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
            return CompletableFuture.completedFuture(response);
        });

        ListingPage<michaelcirkl.ubsa.Bucket> buckets = adapter.listBuckets(PageRequest.builder().pageSize(5).build()).join();

        assertEquals(1, buckets.getItems().size());
        michaelcirkl.ubsa.Bucket bucket = buckets.getItems().get(0);
        assertEquals("bucket", bucket.getName());
        assertEquals(java.time.LocalDateTime.of(2025, 1, 2, 3, 4, 5), bucket.getCreationDate());
        assertNull(bucket.getLastModified());
        assertTrue(buckets.hasNextPage());
        assertEquals("page-2", buckets.getNextContinuationToken());
    }

    @Test
    void getBlobWrapsS3FailuresInUbsaException() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        S3Exception failure = s3Exception(500, "boom");
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn(CompletableFuture.failedFuture(failure));

        CompletionException completionError = assertThrows(CompletionException.class, () -> adapter.getBlob("bucket", "blob").join());
        assertTrue(completionError.getCause() instanceof UbsaException);
        UbsaException error = (UbsaException) completionError.getCause();
        assertEquals("boom", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void deleteBucketIfExistsIgnoresNotFound() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        when(client.deleteBucket(any(DeleteBucketRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(s3Exception(404, "missing")));

        assertDoesNotThrow(() -> adapter.deleteBucketIfExists("bucket").join());
    }

    @Test
    void deleteBucketIfExistsWrapsNonNotFoundFailures() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        S3Exception failure = s3Exception(500, "boom");
        when(client.deleteBucket(any(DeleteBucketRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(failure));

        CompletionException completionError = assertThrows(CompletionException.class, () -> adapter.deleteBucketIfExists("bucket").join());
        assertTrue(completionError.getCause() instanceof UbsaException);
        UbsaException error = (UbsaException) completionError.getCause();
        assertEquals("boom", error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void generateGetUrlWrapsPresignerFailuresInUbsaException() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        S3Exception failure = s3Exception(503, "presign boom");
        when(client.serviceClientConfiguration()).thenThrow(failure);

        UbsaException error = assertThrows(
                UbsaException.class,
                () -> adapter.generateGetUrl("bucket", "blob", java.time.Duration.ofMinutes(1))
        );
        assertEquals("presign boom", error.getMessage());
        assertEquals(503, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void getByteRangeRejectsInvalidRangesBeforeCallingSdk() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> adapter.getByteRange("bucket", "blob", -1, 5)
        );

        assertEquals("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.", error.getMessage());
        verifyNoInteractions(client);
    }

    @Test
    void openBlobStreamWrapsOpenFailuresInUbsaException() throws Exception {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        S3Exception failure = s3Exception(404, "missing");
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn(CompletableFuture.failedFuture(failure));

        Throwable error = streamError(adapter.openBlobStream("bucket", "blob"));

        assertTrue(error instanceof UbsaException);
        UbsaException ubsaError = (UbsaException) error;
        assertEquals("missing", ubsaError.getMessage());
        assertEquals(404, ubsaError.getStatusCode());
        assertSame(failure, ubsaError.getCause());
    }

    @Test
    void listBlobsReturnsSinglePageAndContinuationToken() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);

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
                    return CompletableFuture.completedFuture(request.continuationToken() == null ? firstPage : secondPage);
                });

        ListingPage<Blob> blobs = adapter.listBlobs("bucket", "prefix/", PageRequest.builder().pageSize(1).build()).join();

        assertEquals(List.of("prefix/blob-1"), blobs.getItems().stream().map(Blob::getKey).toList());
        assertEquals("page-2", blobs.getNextContinuationToken());

        ListingPage<Blob> secondResult = adapter.listBlobs(
                "bucket",
                "prefix/",
                PageRequest.builder().pageSize(1).continuationToken("page-2").build()
        ).join();

        assertEquals(List.of("prefix/blob-2"), secondResult.getItems().stream().map(Blob::getKey).toList());
        assertEquals(null, secondResult.getNextContinuationToken());
    }

    @Test
    void streamBlobsTraversesMultiplePages() throws Exception {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);

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
                    return CompletableFuture.completedFuture(request.continuationToken() == null ? firstPage : secondPage);
                });

        List<String> keys = collectPublisherKeys(
                adapter.streamBlobs("bucket", "prefix/", 1)
        );

        assertEquals(List.of("prefix/blob-1", "prefix/blob-2"), keys);
    }

    @Test
    void streamBlobsRejectsInvalidPageSize() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> adapter.streamBlobs("bucket", "prefix/", 0)
        );

        assertEquals("Page size must be greater than 0.", error.getMessage());
    }

    private static S3Exception s3Exception(int statusCode, String message) {
        S3Exception.Builder builder = S3Exception.builder();
        builder.statusCode(statusCode);
        builder.message(message);
        return (S3Exception) builder.build();
    }

    private static Throwable streamError(Flow.Publisher<ByteBuffer> publisher) throws Exception {
        CompletableFuture<Throwable> future = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                future.complete(throwable);
            }

            @Override
            public void onComplete() {
                future.completeExceptionally(new AssertionError("Expected stream failure"));
            }
        });
        return future.get(5, TimeUnit.SECONDS);
    }

    private static List<String> collectPublisherKeys(Flow.Publisher<Blob> publisher) throws Exception {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        List<String> keys = new java.util.ArrayList<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Blob item) {
                keys.add(item.getKey());
            }

            @Override
            public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                future.complete(keys);
            }
        });
        return future.get(5, TimeUnit.SECONDS);
    }
}
