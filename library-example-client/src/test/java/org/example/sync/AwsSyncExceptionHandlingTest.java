package org.example.sync;

import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.sync.AWSSyncClientImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsSyncExceptionHandlingTest {
    @Test
    void getBlobWrapsS3FailuresInUbsaException() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        S3Exception failure = s3Exception(500);
        when(client.getObjectAsBytes(any(GetObjectRequest.class))).thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.getBlob("bucket", "blob"));
        assertEquals(failure.getMessage(), error.getMessage());
        assertNull(error.getCause());
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
        assertNull(error.getCause());
    }

    private static S3Exception s3Exception(int statusCode) {
        S3Exception.Builder builder = S3Exception.builder();
        builder.statusCode(statusCode);
        return (S3Exception) builder.build();
    }
}
