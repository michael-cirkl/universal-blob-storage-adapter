package org.example.createifnotexists;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.UbsaException;
import michaelcirkl.ubsa.client.async.AzureAsyncClientImpl;
import michaelcirkl.ubsa.client.sync.AzureSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

class AzureCreateBlobIfNotExistsTest {
    @Test
    void syncCreatesWhenAbsentUsingIfNoneMatch() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        Response<BlockBlobItem> response = mock(Response.class);
        BlockBlobItem item = mock(BlockBlobItem.class);

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("key")).thenReturn(blobClient);
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), isNull())).thenReturn(response);
        when(response.getValue()).thenReturn(item);
        when(item.getETag()).thenReturn("new-etag");

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob());
        assertEquals("new-etag", etag);

        ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
        verify(blobClient).uploadWithResponse(optionsCaptor.capture(), isNull(), isNull());
        assertNotNull(optionsCaptor.getValue().getRequestConditions());
        assertEquals("*", optionsCaptor.getValue().getRequestConditions().getIfNoneMatch());
        verify(blobClient, never()).exists();
    }

    @Test
    void syncReturnsExistingEtagOnConflict() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        BlobStorageException conflict = mock(BlobStorageException.class);
        BlobProperties properties = mock(BlobProperties.class);

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("key")).thenReturn(blobClient);
        when(conflict.getStatusCode()).thenReturn(412);
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), isNull())).thenThrow(conflict);
        when(blobClient.getProperties()).thenReturn(properties);
        when(properties.getETag()).thenReturn("existing-etag");

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob());
        assertEquals("existing-etag", etag);
    }

    @Test
    void syncWrapsNonConflictFailures() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        BlobStorageException failure = mock(BlobStorageException.class);

        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobClient("key")).thenReturn(blobClient);
        when(failure.getStatusCode()).thenReturn(500);
        when(failure.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), isNull())).thenThrow(failure);

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        UbsaException error = assertThrows(UbsaException.class, () -> adapter.createBlobIfNotExists("bucket", sampleBlob()));
        assertSame(failure, error.getCause());
        verify(blobClient, never()).getProperties();
    }

    @Test
    void asyncCreatesWhenAbsentUsingIfNoneMatch() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        Response<BlockBlobItem> response = mock(Response.class);
        BlockBlobItem item = mock(BlockBlobItem.class);

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("key")).thenReturn(blobClient);
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class))).thenReturn(Mono.just(response));
        when(response.getValue()).thenReturn(item);
        when(item.getETag()).thenReturn("new-etag");

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob()).get();
        assertEquals("new-etag", etag);

        ArgumentCaptor<BlobParallelUploadOptions> optionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
        verify(blobClient).uploadWithResponse(optionsCaptor.capture());
        assertNotNull(optionsCaptor.getValue().getRequestConditions());
        assertEquals("*", optionsCaptor.getValue().getRequestConditions().getIfNoneMatch());
        verify(blobClient, never()).exists();
    }

    @Test
    void asyncReturnsExistingEtagOnConflict() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        BlobStorageException conflict = mock(BlobStorageException.class);
        BlobProperties properties = mock(BlobProperties.class);

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("key")).thenReturn(blobClient);
        when(conflict.getStatusCode()).thenReturn(409);
        when(conflict.getErrorCode()).thenReturn(BlobErrorCode.BLOB_ALREADY_EXISTS);
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class))).thenReturn(Mono.error(conflict));
        when(blobClient.getProperties()).thenReturn(Mono.just(properties));
        when(properties.getETag()).thenReturn("existing-etag");

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob()).get();
        assertEquals("existing-etag", etag);
    }

    @Test
    void asyncWrapsNonConflictFailures() {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        BlobStorageException failure = mock(BlobStorageException.class);

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobAsyncClient("key")).thenReturn(blobClient);
        when(failure.getStatusCode()).thenReturn(500);
        when(failure.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class))).thenReturn(Mono.error(failure));

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        ExecutionException error = assertThrows(
                ExecutionException.class,
                () -> adapter.createBlobIfNotExists("bucket", sampleBlob()).get()
        );
        Throwable unwrapped = unwrapFutureFailure(error);
        assertTrue(unwrapped instanceof UbsaException);
        assertSame(failure, unwrapped.getCause());
        verify(blobClient, never()).getProperties();
    }

    private static Blob sampleBlob() {
        return Blob.builder()
                .bucket("bucket")
                .key("key")
                .content("data".getBytes())
                .build();
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
