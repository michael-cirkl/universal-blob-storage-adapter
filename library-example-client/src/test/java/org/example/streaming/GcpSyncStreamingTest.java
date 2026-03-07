package org.example.streaming;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.sync.GCPSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcpSyncStreamingTest {
    @Test
    void openBlobStreamReadsFromReadChannel() throws Exception {
        Storage storage = mock(Storage.class);
        byte[] expected = "gcp-sync-open".getBytes();
        when(storage.reader(ArgumentMatchers.any())).thenReturn(new StreamingTestSupport.ByteArrayReadChannel(expected));

        GCPSyncClientImpl adapter = new GCPSyncClientImpl(storage);
        InputStream inputStream = adapter.openBlobStream("bucket", "blob");
        assertArrayEquals(expected, StreamingTestSupport.readAll(inputStream));
    }

    @Test
    void createBlobStreamingWritesAllBytesAndReturnsEtag() throws Exception {
        Storage storage = mock(Storage.class);
        CapturingWriteChannel writeChannel = new CapturingWriteChannel();
        when(storage.writer(ArgumentMatchers.any(com.google.cloud.storage.BlobInfo.class))).thenReturn(writeChannel);

        com.google.cloud.storage.Blob created = mock(com.google.cloud.storage.Blob.class);
        when(storage.get("bucket", "blob")).thenReturn(created);
        when(created.getEtag()).thenReturn("etag-gcp-sync");

        GCPSyncClientImpl adapter = new GCPSyncClientImpl(storage);
        byte[] expected = "gcp-sync-streaming".getBytes();
        String etag = adapter.createBlob(
                "bucket",
                "blob",
                new ByteArrayInputStream(expected),
                expected.length,
                BlobWriteOptions.builder().encoding("gzip").userMetadata(Map.of("k", "v")).build()
        );

        assertEquals("etag-gcp-sync", etag);
        assertArrayEquals(expected, writeChannel.toByteArray());
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

        byte[] toByteArray() {
            return output.toByteArray();
        }
    }
}
