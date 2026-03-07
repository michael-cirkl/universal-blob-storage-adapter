package org.example.streaming;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobProperties;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.sync.AzureSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureSyncStreamingTest {
    @Test
    void openBlobStreamStreamsDataFromBlobClient() throws Exception {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("blob")).thenReturn(blobClient);
        BlobInputStream blobInputStream = mock(BlobInputStream.class);
        byte[] data = "azure-sync".getBytes();
        ByteArrayInputStream source = new ByteArrayInputStream(data);
        when(blobClient.openInputStream()).thenReturn(blobInputStream);
        when(blobInputStream.read(any(byte[].class)))
                .thenAnswer(invocation -> source.read(invocation.getArgument(0)));
        when(blobInputStream.read(any(byte[].class), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt()))
                .thenAnswer(invocation -> source.read(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2)));
        when(blobInputStream.read()).thenAnswer(invocation -> source.read());

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        InputStream stream = adapter.openBlobStream("bucket", "blob");
        assertArrayEquals(data, StreamingTestSupport.readAll(stream));
    }

    @Test
    void createBlobStreamingUploadsHeadersAndMetadataAtomically() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        BlobProperties properties = mock(BlobProperties.class);
        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("blob")).thenReturn(blobClient);
        when(blobClient.getProperties()).thenReturn(properties);
        when(properties.getETag()).thenReturn("etag-azure-sync");

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        byte[] data = "azure-stream-content".getBytes();
        BlobWriteOptions options = BlobWriteOptions.builder()
                .encoding("gzip")
                .userMetadata(Map.of("k", "v"))
                .build();

        String etag = adapter.createBlob("bucket", "blob", new ByteArrayInputStream(data), data.length, options);
        assertEquals("etag-azure-sync", etag);

        ArgumentCaptor<BlobHttpHeaders> headersCaptor = ArgumentCaptor.forClass(BlobHttpHeaders.class);
        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(blobClient).uploadWithResponse(any(InputStream.class), eq((long) data.length), eq(null),
                headersCaptor.capture(), metadataCaptor.capture(), eq(null), eq(null), eq(null), eq(Context.NONE));
        assertNotNull(headersCaptor.getValue());
        assertEquals("gzip", headersCaptor.getValue().getContentEncoding());
        assertEquals("v", metadataCaptor.getValue().get("k"));
        verify(blobClient, never()).setHttpHeaders(any());
        verify(blobClient, never()).setMetadata(any());
    }
}
