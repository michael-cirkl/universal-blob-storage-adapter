package org.example.createifnotexists;

import com.google.api.core.ApiFutures;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.UbsaException;
import michaelcirkl.ubsa.client.async.GCPAsyncClientImpl;
import michaelcirkl.ubsa.client.sync.GCPSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GcpCreateBlobIfNotExistsTest {
    @Test
    void syncCreatesWhenAbsentWithDoesNotExistOption() {
        Storage storage = mock(Storage.class);
        CapturingWriteChannel writeChannel = new CapturingWriteChannel();
        com.google.cloud.storage.Blob created = mock(com.google.cloud.storage.Blob.class);
        when(storage.writer(any(com.google.cloud.storage.BlobInfo.class), any(Storage.BlobWriteOption[].class)))
                .thenReturn(writeChannel);
        when(storage.get("bucket", "key")).thenReturn(created);
        when(created.getEtag()).thenReturn("new-etag");

        GCPSyncClientImpl adapter = new GCPSyncClientImpl(storage);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob());
        assertEquals("new-etag", etag);

        ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor = ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
        verify(storage).writer(any(com.google.cloud.storage.BlobInfo.class), optionsCaptor.capture());
        assertEquals(1, optionsCaptor.getValue().length);
    }

    @Test
    void syncReturnsExistingEtagOnConflict() {
        Storage storage = mock(Storage.class);
        StorageException conflict = new StorageException(412, "precondition");
        com.google.cloud.storage.Blob existing = mock(com.google.cloud.storage.Blob.class);
        when(storage.writer(any(com.google.cloud.storage.BlobInfo.class), any(Storage.BlobWriteOption[].class)))
                .thenThrow(conflict);
        when(storage.get("bucket", "key")).thenReturn(existing);
        when(existing.getEtag()).thenReturn("existing-etag");

        GCPSyncClientImpl adapter = new GCPSyncClientImpl(storage);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob());
        assertEquals("existing-etag", etag);
    }

    @Test
    void syncWrapsNonConflictFailures() {
        Storage storage = mock(Storage.class);
        StorageException failure = new StorageException(500, "boom");
        when(storage.writer(any(com.google.cloud.storage.BlobInfo.class), any(Storage.BlobWriteOption[].class)))
                .thenThrow(failure);

        GCPSyncClientImpl adapter = new GCPSyncClientImpl(storage);
        UbsaException error = assertThrows(UbsaException.class, () -> adapter.createBlobIfNotExists("bucket", sampleBlob()));
        assertSame(failure, error.getCause());
        verify(storage, never()).get("bucket", "key");
    }

    @Test
    void asyncCreatesWhenAbsentWithDoesNotExistOption() throws Exception {
        Storage storage = mock(Storage.class);
        BlobWriteSession writeSession = mock(BlobWriteSession.class);
        CapturingWritableByteChannel channel = new CapturingWritableByteChannel();
        com.google.cloud.storage.BlobInfo created = mock(com.google.cloud.storage.BlobInfo.class);

        when(storage.blobWriteSession(any(com.google.cloud.storage.BlobInfo.class), any(Storage.BlobWriteOption[].class)))
                .thenReturn(writeSession);
        when(writeSession.open()).thenReturn(channel);
        when(writeSession.getResult()).thenReturn(ApiFutures.immediateFuture(created));
        when(created.getEtag()).thenReturn("new-etag");

        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(storage);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob()).get();
        assertEquals("new-etag", etag);

        ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor = ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
        verify(storage).blobWriteSession(any(com.google.cloud.storage.BlobInfo.class), optionsCaptor.capture());
        assertEquals(1, optionsCaptor.getValue().length);
        verify(storage, never()).get("bucket", "key");
    }

    @Test
    void asyncReturnsExistingEtagOnConflict() throws Exception {
        Storage storage = mock(Storage.class);
        StorageException conflict = new StorageException(412, "precondition");
        com.google.cloud.storage.Blob existing = mock(com.google.cloud.storage.Blob.class);

        when(storage.blobWriteSession(any(com.google.cloud.storage.BlobInfo.class), any(Storage.BlobWriteOption[].class)))
                .thenThrow(conflict);
        when(storage.get("bucket", "key")).thenReturn(existing);
        when(existing.getEtag()).thenReturn("existing-etag");

        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(storage);
        String etag = adapter.createBlobIfNotExists("bucket", sampleBlob()).get();
        assertEquals("existing-etag", etag);
    }

    @Test
    void asyncWrapsNonConflictFailures() {
        Storage storage = mock(Storage.class);
        StorageException failure = new StorageException(500, "boom");
        when(storage.blobWriteSession(any(com.google.cloud.storage.BlobInfo.class), any(Storage.BlobWriteOption[].class)))
                .thenThrow(failure);

        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(storage);
        ExecutionException error = assertThrows(
                ExecutionException.class,
                () -> adapter.createBlobIfNotExists("bucket", sampleBlob()).get()
        );
        Throwable unwrapped = unwrapFutureFailure(error);
        assertTrue(unwrapped instanceof UbsaException);
        assertSame(failure, unwrapped.getCause());
        verify(storage, never()).get("bucket", "key");
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

    private static final class CapturingWriteChannel implements WriteChannel {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private boolean open = true;

        @Override
        public int write(ByteBuffer src) throws java.io.IOException {
            if (!open) {
                throw new ClosedChannelException();
            }
            int len = src.remaining();
            byte[] bytes = new byte[len];
            src.get(bytes);
            output.write(bytes);
            return len;
        }

        @Override
        public void setChunkSize(int chunkSize) {
            // no-op
        }

        @Override
        public com.google.cloud.RestorableState<WriteChannel> capture() {
            throw new UnsupportedOperationException("Not needed for tests.");
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }

    private static final class CapturingWritableByteChannel implements WritableByteChannel {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private boolean open = true;

        @Override
        public int write(ByteBuffer src) throws java.io.IOException {
            if (!open) {
                throw new ClosedChannelException();
            }
            int len = src.remaining();
            byte[] bytes = new byte[len];
            src.get(bytes);
            output.write(bytes);
            return len;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
