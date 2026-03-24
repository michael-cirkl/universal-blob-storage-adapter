package org.example.sync;

import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.aws.AWSSyncClientImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AwsSyncExceptionHandlingTest {
    @Test
    void getBlobMetadataReadsHeadOnly() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        HeadObjectResponse response = HeadObjectResponse.builder()
                .contentLength(5L)
                .lastModified(java.time.Instant.parse("2025-01-02T03:04:05Z"))
                .contentEncoding("gzip")
                .eTag("etag-1")
                .metadata(Map.of("k", "v"))
                .expiresString("Thu, 02 Jan 2025 04:05:06 GMT")
                .build();
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(response);

        michaelcirkl.ubsa.Blob blob = adapter.getBlobMetadata("bucket", "blob");

        assertNull(blob.getContent());
        assertEquals("bucket", blob.getBucket());
        assertEquals("blob", blob.getKey());
        assertEquals(5L, blob.getSize());
        assertEquals("gzip", blob.encoding());
        assertEquals("etag-1", blob.getEtag());
        assertEquals(Map.of("k", "v"), blob.getUserMetadata());
        assertEquals(URI.create("s3://bucket/blob"), blob.getPublicURI());
        assertEquals(LocalDateTime.of(2025, 1, 2, 3, 4, 5), blob.lastModified());
        assertEquals(LocalDateTime.of(2025, 1, 2, 4, 5, 6), blob.expires());
        verify(client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    @Test
    void getBlobWrapsS3FailuresInUbsaException() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        S3Exception failure = s3Exception(500);
        when(client.getObjectAsBytes(any(GetObjectRequest.class))).thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlob("bucket", "blob"));
        assertEquals(failure.getMessage(), error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void getBlobMetadataWrapsS3FailuresInUbsaException() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        S3Exception failure = s3Exception(500);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlobMetadata("bucket", "blob"));
        assertEquals(failure.getMessage(), error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    @Test
    void deleteBucketIfExistsIgnoresNotFound() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        when(client.deleteBucket(any(DeleteBucketRequest.class))).thenThrow(s3Exception(404));

        assertDoesNotThrow(() -> adapter.deleteBucketIfExists("bucket"));
    }

    @Test
    void deleteBucketIfExistsWrapsNonNotFoundFailures() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        S3Exception failure = s3Exception(500);
        when(client.deleteBucket(any(DeleteBucketRequest.class))).thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.deleteBucketIfExists("bucket"));
        assertEquals(failure.getMessage(), error.getMessage());
        assertEquals(500, error.getStatusCode());
        assertSame(failure, error.getCause());
    }

    private static S3Exception s3Exception(int statusCode) {
        S3Exception.Builder builder = S3Exception.builder();
        builder.statusCode(statusCode);
        builder.message("aws error " + statusCode);
        return (S3Exception) builder.build();
    }
}
