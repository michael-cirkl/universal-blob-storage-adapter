package michaelcirkl.ubsa.client.streaming;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.*;

class GcpReadChannelFlowPublisherTest {
    @Test
    void publishesReadChannelContent() throws Exception {
        byte[] expected = "gcp-read-channel".getBytes();
        Storage storage = storageWithReader(expected);
        GcpReadChannelFlowPublisher publisher = new GcpReadChannelFlowPublisher(storage, BlobId.of("bucket", "blob"), Runnable::run);
        byte[] received = collect(publisher);
        assertArrayEquals(expected, received);
    }

    @Test
    void rejectsNonPositiveDemand() {
        Storage storage = storageWithReader("ignored".getBytes());
        GcpReadChannelFlowPublisher publisher = new GcpReadChannelFlowPublisher(storage, BlobId.of("bucket", "blob"), Runnable::run);

        CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(0);
            }

            @Override
            public void onNext(ByteBuffer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                errorFuture.complete(throwable);
            }

            @Override
            public void onComplete() {
            }
        });

        Throwable error = errorFuture.join();
        assertNotNull(error);
        assertTrue(error instanceof IllegalArgumentException);
    }

    private static byte[] collect(Flow.Publisher<ByteBuffer> publisher) throws Exception {
        CompletableFuture<byte[]> done = new CompletableFuture<>();
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                ByteBuffer copy = item.slice();
                byte[] bytes = new byte[copy.remaining()];
                copy.get(bytes);
                output.writeBytes(bytes);
            }

            @Override
            public void onError(Throwable throwable) {
                done.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                done.complete(output.toByteArray());
            }
        });
        return done.get();
    }

    private static Storage storageWithReader(byte[] content) {
        return (Storage) Proxy.newProxyInstance(
                Storage.class.getClassLoader(),
                new Class[]{Storage.class},
                (proxy, method, args) -> {
                    if ("reader".equals(method.getName())) {
                        return new ByteArrayReadChannel(content);
                    }
                    if ("toString".equals(method.getName())) {
                        return "storageWithReader";
                    }
                    throw new UnsupportedOperationException("Not implemented in test: " + method.getName());
                }
        );
    }

    private static final class ByteArrayReadChannel implements ReadChannel {
        private final byte[] data;
        private int index;
        private boolean open = true;

        private ByteArrayReadChannel(byte[] data) {
            this.data = java.util.Arrays.copyOf(data, data.length);
        }

        @Override
        public int read(ByteBuffer dst) {
            if (!open) {
                return -1;
            }
            if (index >= data.length) {
                return -1;
            }
            int len = Math.min(dst.remaining(), data.length - index);
            dst.put(data, index, len);
            index += len;
            return len;
        }

        @Override
        public void seek(long position) {
            index = (int) position;
        }

        @Override
        public void setChunkSize(int chunkSize) {
            // no-op
        }

        @Override
        public com.google.cloud.RestorableState<ReadChannel> capture() {
            throw new UnsupportedOperationException("Not needed in tests.");
        }

        @Override
        public void close() {
            open = false;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public ReadChannel limit(long limit) {
            return this;
        }

        @Override
        public long limit() {
            return data.length;
        }
    }
}
