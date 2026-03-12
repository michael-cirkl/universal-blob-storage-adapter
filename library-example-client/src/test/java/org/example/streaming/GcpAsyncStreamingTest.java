package org.example.streaming;

import com.google.api.core.ApiFutures;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.client.async.GCPAsyncClientImpl;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcpAsyncStreamingTest {
    @Test
    void openBlobStreamPublishesAllBytes() throws Exception {
        Storage storage = mock(Storage.class);
        byte[] expected = "gcp-async-open".getBytes();
        when(storage.reader(ArgumentMatchers.any())).thenReturn(new StreamingTestSupport.ByteArrayReadChannel(expected));

        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(storage);
        Flow.Publisher<ByteBuffer> publisher = adapter.openBlobStream("bucket", "blob").get();
        assertArrayEquals(expected, StreamingTestSupport.collectFlowPublisher(publisher));
    }

    @Test
    void createBlobStreamingWritesPublisherBytesToBlobWriteSession() throws Exception {
        Storage storage = mock(Storage.class);
        BlobWriteSession writeSession = mock(BlobWriteSession.class);
        CapturingWritableByteChannel channel = new CapturingWritableByteChannel();
        when(storage.blobWriteSession(ArgumentMatchers.any())).thenReturn(writeSession);
        when(writeSession.open()).thenReturn(channel);
        com.google.cloud.storage.BlobInfo result = mock(com.google.cloud.storage.BlobInfo.class);
        when(result.getEtag()).thenReturn("etag-gcp-async");
        when(writeSession.getResult()).thenReturn(ApiFutures.immediateFuture(result));

        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(storage);
        byte[] first = "gcp".getBytes();
        byte[] second = "-async".getBytes();
        String etag = adapter.createBlob(
                "bucket",
                "blob",
                StreamingTestSupport.flowPublisher(first, second),
                first.length + second.length,
                BlobWriteOptions.builder().build()
        ).get();

        assertEquals("etag-gcp-async", etag);
        assertArrayEquals("gcp-async".getBytes(), channel.data());
    }

    @Test
    void createBlobFromFileUsesStorageCreateFrom(@TempDir Path tempDir) throws Exception {
        Storage storage = mock(Storage.class);
        Path sourceFile = Files.writeString(tempDir.resolve("gcp-async.txt"), "gcp-async-file");
        com.google.cloud.storage.Blob created = mock(com.google.cloud.storage.Blob.class);
        when(created.getEtag()).thenReturn("etag-gcp-async-file");
        when(storage.createFrom(any(BlobInfo.class), any(Path.class)))
                .thenReturn(created);

        GCPAsyncClientImpl adapter = new GCPAsyncClientImpl(storage);
        String etag = adapter.createBlob("bucket", "blob", sourceFile).get();
        assertEquals("etag-gcp-async-file", etag);

        ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        org.mockito.Mockito.verify(storage).createFrom(blobInfoCaptor.capture(), pathCaptor.capture());
        assertEquals("bucket", blobInfoCaptor.getValue().getBucket());
        assertEquals("blob", blobInfoCaptor.getValue().getName());
        assertEquals(sourceFile, pathCaptor.getValue());
    }

    private static final class CapturingWritableByteChannel implements WritableByteChannel {
        private final java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
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

        byte[] data() {
            return output.toByteArray();
        }
    }
}
