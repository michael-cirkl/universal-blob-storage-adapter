package org.example.streaming;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import michaelcirkl.ubsa.client.async.AzureAsyncClientImpl;
import michaelcirkl.ubsa.client.async.BlobWriteOptions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureAsyncStreamingTest {
    @Test
    void openBlobStreamPublishesAllChunks() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerAsyncClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobAsyncClient = mock(BlobAsyncClient.class);
        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerAsyncClient);
        when(containerAsyncClient.getBlobAsyncClient("blob")).thenReturn(blobAsyncClient);
        when(blobAsyncClient.downloadStream()).thenReturn(Flux.just(ByteBuffer.wrap("azure-".getBytes()), ByteBuffer.wrap("async".getBytes())));

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        Flow.Publisher<ByteBuffer> publisher = adapter.openBlobStream("bucket", "blob").get();
        assertArrayEquals("azure-async".getBytes(), StreamingTestSupport.collectFlowPublisher(publisher));
    }

    @Test
    void createBlobStreamingPassesLengthAndOptionsToBlockUpload() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerAsyncClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobAsyncClient = mock(BlobAsyncClient.class);
        BlockBlobAsyncClient blockBlobAsyncClient = mock(BlockBlobAsyncClient.class);
        Response<BlockBlobItem> response = mock(Response.class);
        BlockBlobItem item = mock(BlockBlobItem.class);

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerAsyncClient);
        when(containerAsyncClient.getBlobAsyncClient("blob")).thenReturn(blobAsyncClient);
        when(blobAsyncClient.getBlockBlobAsyncClient()).thenReturn(blockBlobAsyncClient);
        when(blockBlobAsyncClient.uploadWithResponse(any(Flux.class), eq(5L), any(), any(), eq(null), eq(null), eq(null)))
                .thenReturn(Mono.just(response));
        when(response.getValue()).thenReturn(item);
        when(item.getETag()).thenReturn("etag-azure-async");

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        String etag = adapter.createBlob(
                "bucket",
                "blob",
                StreamingTestSupport.flowPublisher("he".getBytes(), "llo".getBytes()),
                5,
                BlobWriteOptions.builder().encoding("gzip").userMetadata(Map.of("k", "v")).build()
        ).get();
        assertEquals("etag-azure-async", etag);

        ArgumentCaptor<BlobHttpHeaders> headersCaptor = ArgumentCaptor.forClass(BlobHttpHeaders.class);
        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(blockBlobAsyncClient).uploadWithResponse(any(Flux.class), eq(5L), headersCaptor.capture(), metadataCaptor.capture(),
                eq(null), eq(null), eq(null));
        assertNotNull(headersCaptor.getValue());
        assertEquals("gzip", headersCaptor.getValue().getContentEncoding());
        assertEquals("v", metadataCaptor.getValue().get("k"));
    }
}
