package org.example.streaming;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.sync.AzureSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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

        ArgumentCaptor<BlobParallelUploadOptions> uploadOptionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
        verify(blobClient).uploadWithResponse(uploadOptionsCaptor.capture(), eq(null), eq(Context.NONE));
        BlobParallelUploadOptions uploadOptions = uploadOptionsCaptor.getValue();
        assertNotNull(uploadOptions);
        assertNotNull(uploadOptions.getHeaders());
        assertEquals("gzip", uploadOptions.getHeaders().getContentEncoding());
        assertNotNull(uploadOptions.getMetadata());
        assertEquals("v", uploadOptions.getMetadata().get("k"));
        verify(blobClient, never()).setHttpHeaders(any());
        verify(blobClient, never()).setMetadata(any());
    }

    @Test
    void createBlobFromFileUsesAzureUploadFromFile(@TempDir Path tempDir) throws Exception {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        Response<BlockBlobItem> response = mock(Response.class);
        BlockBlobItem item = mock(BlockBlobItem.class);
        Path sourceFile = Files.writeString(tempDir.resolve("azure-sync.txt"), "azure-sync-file");

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("blob")).thenReturn(blobClient);
        when(blobClient.uploadFromFileWithResponse(any(BlobUploadFromFileOptions.class), eq(null), eq(Context.NONE)))
                .thenReturn(response);
        when(response.getValue()).thenReturn(item);
        when(item.getETag()).thenReturn("etag-azure-sync-file");

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        String etag = adapter.createBlob("bucket", "blob", sourceFile);
        assertEquals("etag-azure-sync-file", etag);

        ArgumentCaptor<BlobUploadFromFileOptions> optionsCaptor = ArgumentCaptor.forClass(BlobUploadFromFileOptions.class);
        verify(blobClient).uploadFromFileWithResponse(optionsCaptor.capture(), eq(null), eq(Context.NONE));
        assertEquals(sourceFile.toString(), optionsCaptor.getValue().getFilePath());
    }

    @Test
    void createBlobFromFileRejectsMissingSourceFile(@TempDir Path tempDir) {
        AzureSyncClientImpl adapter = new AzureSyncClientImpl(mock(BlobServiceClient.class));
        Path missingFile = tempDir.resolve("missing-file.txt");
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> adapter.createBlob("bucket", "blob", missingFile)
        );
        assertTrue(error.getMessage().contains("does not exist"));
    }
}
