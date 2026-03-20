package org.example.async;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.UbsaException;
import michaelcirkl.ubsa.client.async.AWSAsyncClientImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsAsyncExceptionHandlingTest {
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
        assertEquals(failure.getMessage(), error.getMessage());
        assertEquals(500, error.getStatusCode());
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
        assertEquals(failure.getMessage(), error.getMessage());
        assertEquals(500, error.getStatusCode());
    }

    @Test
    void listBlobsByPrefixReadsAllPages() {
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

        Set<Blob> blobs = adapter.listBlobsByPrefix("bucket", "prefix/").join();

        assertEquals(2, blobs.size());
        assertTrue(blobs.stream().map(Blob::getKey).anyMatch("prefix/blob-1"::equals));
        assertTrue(blobs.stream().map(Blob::getKey).anyMatch("prefix/blob-2"::equals));
    }

    private static S3Exception s3Exception(int statusCode, String message) {
        S3Exception.Builder builder = S3Exception.builder();
        builder.statusCode(statusCode);
        builder.message(message);
        return (S3Exception) builder.build();
    }
}
