package org.example.createifnotexists;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.UbsaException;
import michaelcirkl.ubsa.client.async.AWSAsyncClientImpl;
import michaelcirkl.ubsa.client.sync.AWSSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AwsCreateBlobIfNotExistsTest {
    @Test
    void syncCreatesWhenAbsentUsingIfNoneMatch() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("new-etag").build());

        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob());
        assertEquals("new-etag", etag);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(client).putObject(requestCaptor.capture(), any(RequestBody.class));
        assertEquals("*", requestCaptor.getValue().ifNoneMatch());
        verify(client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void syncReturnsExistingEtagOnConflict() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(s3Exception(412));
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().eTag("existing-etag").build());

        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob());
        assertEquals("existing-etag", etag);
    }

    @Test
    void syncWrapsNonConflictFailures() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);
        S3Exception failure = s3Exception(500);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(failure);

        UbsaException error = assertThrows(UbsaException.class, () -> adapter.createBlobIfNotExists("bucket", sampleBlob()));
        assertSame(failure, error.getCause());
        verify(client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void asyncCreatesWhenAbsentUsingIfNoneMatch() throws Exception {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().eTag("new-etag").build()));

        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob()).get();
        assertEquals("new-etag", etag);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(client).putObject(requestCaptor.capture(), any(AsyncRequestBody.class));
        assertEquals("*", requestCaptor.getValue().ifNoneMatch());
        verify(client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void asyncReturnsExistingEtagOnConflict() throws Exception {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.failedFuture(s3Exception(412)));
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().eTag("existing-etag").build()));

        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob()).get();
        assertEquals("existing-etag", etag);
    }

    @Test
    void asyncWrapsNonConflictFailures() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        S3Exception failure = s3Exception(500);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.failedFuture(failure));

        ExecutionException error = assertThrows(
                ExecutionException.class,
                () -> adapter.createBlobIfNotExists("bucket", sampleBlob()).get()
        );
        Throwable unwrapped = unwrapFutureFailure(error);
        assertTrue(unwrapped instanceof UbsaException);
        assertSame(failure, unwrapped.getCause());
        verify(client, never()).headObject(any(HeadObjectRequest.class));
    }

    private static Blob sampleBlob() {
        return Blob.builder()
                .bucket("bucket")
                .key("key")
                .content("data".getBytes())
                .build();
    }

    private static S3Exception s3Exception(int statusCode) {
        S3Exception.Builder builder = S3Exception.builder();
        builder.statusCode(statusCode);
        return (S3Exception) builder.build();
    }

    private static Throwable unwrapFutureFailure(Throwable error) {
        Throwable current = error;
        while ((current instanceof ExecutionException || current instanceof CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
