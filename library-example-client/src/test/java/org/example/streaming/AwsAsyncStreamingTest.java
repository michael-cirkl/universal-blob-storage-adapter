package org.example.streaming;

import michaelcirkl.ubsa.client.async.AWSAsyncClientImpl;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsAsyncStreamingTest {
    @Test
    void openBlobStreamPublishesAllChunks() throws Exception {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);

        byte[] first = "aws-".getBytes();
        byte[] second = "async".getBytes();
        ResponsePublisher<GetObjectResponse> publisher = new ResponsePublisher<>(
                GetObjectResponse.builder().build(),
                SdkPublisher.fromIterable(java.util.List.of(ByteBuffer.wrap(first), ByteBuffer.wrap(second)))
        );
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(publisher));

        Flow.Publisher<ByteBuffer> opened = adapter.openBlobStream("bucket", "blob").get();
        assertArrayEquals("aws-async".getBytes(), StreamingTestSupport.collectFlowPublisher(opened));
    }

    @Test
    void createBlobStreamingEnforcesContentLengthAndSetsRequestLength() throws Exception {
        S3AsyncClient client = mock(S3AsyncClient.class);
        AWSAsyncClientImpl adapter = new AWSAsyncClientImpl(client);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().eTag("etag-aws-async").build()));

        byte[] first = "abc".getBytes();
        byte[] second = "d".getBytes();
        String etag = adapter.createBlob(
                "bucket",
                "blob",
                StreamingTestSupport.flowPublisher(first, second),
                3,
                BlobWriteOptions.builder().build()
        ).get();
        assertEquals("etag-aws-async", etag);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<AsyncRequestBody> bodyCaptor = ArgumentCaptor.forClass(AsyncRequestBody.class);
        org.mockito.Mockito.verify(client).putObject(requestCaptor.capture(), bodyCaptor.capture());
        assertEquals(3L, requestCaptor.getValue().contentLength());

        StreamingTestSupport.ReactiveResult result = StreamingTestSupport.collectReactivePublisher(bodyCaptor.getValue());
        assertArrayEquals("abc".getBytes(), result.data());
        assertNotNull(result.error());
        assertTrue(result.error() instanceof IllegalArgumentException);
        assertTrue(result.error().getMessage().contains("Content length mismatch"));
    }
}
