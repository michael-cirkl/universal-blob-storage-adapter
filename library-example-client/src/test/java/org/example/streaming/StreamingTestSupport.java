package org.example.streaming;

import com.google.cloud.ReadChannel;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

final class StreamingTestSupport {
    private StreamingTestSupport() {
    }

    static byte[] readAll(InputStream inputStream) throws IOException {
        try (inputStream; ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[256];
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                if (read == 0) {
                    int single = inputStream.read();
                    if (single == -1) {
                        break;
                    }
                    output.write(single);
                    continue;
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    static byte[] collectFlowPublisher(Flow.Publisher<ByteBuffer> publisher) throws Exception {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                ByteBuffer buffer = item.slice();
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                output.writeBytes(chunk);
            }

            @Override
            public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                future.complete(output.toByteArray());
            }
        });
        return future.get(5, TimeUnit.SECONDS);
    }

    static ReactiveResult collectReactivePublisher(Publisher<ByteBuffer> publisher) throws Exception {
        CompletableFuture<ReactiveResult> future = new CompletableFuture<>();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                ByteBuffer buffer = item.slice();
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                output.writeBytes(chunk);
            }

            @Override
            public void onError(Throwable throwable) {
                future.complete(new ReactiveResult(output.toByteArray(), throwable));
            }

            @Override
            public void onComplete() {
                future.complete(new ReactiveResult(output.toByteArray(), null));
            }
        });
        return future.get(5, TimeUnit.SECONDS);
    }

    static Flow.Publisher<ByteBuffer> flowPublisher(byte[]... chunks) {
        byte[][] copy = Arrays.stream(chunks)
                .map(chunk -> Arrays.copyOf(chunk, chunk.length))
                .toArray(byte[][]::new);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean cancelled;

            @Override
            public void request(long n) {
                if (cancelled || n <= 0) {
                    if (!cancelled && n <= 0) {
                        cancelled = true;
                        subscriber.onError(new IllegalArgumentException("Demand must be > 0"));
                    }
                    return;
                }
                long remainingDemand = n;
                while (!cancelled && remainingDemand > 0 && index < copy.length) {
                    subscriber.onNext(ByteBuffer.wrap(copy[index++]));
                    remainingDemand--;
                }
                if (!cancelled && index >= copy.length) {
                    cancelled = true;
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        });
    }

    static final class ReactiveResult {
        private final byte[] data;
        private final Throwable error;

        private ReactiveResult(byte[] data, Throwable error) {
            this.data = data;
            this.error = error;
        }

        byte[] data() {
            return data;
        }

        Throwable error() {
            return error;
        }
    }

    static final class ByteArrayReadChannel implements ReadChannel {
        private final byte[] data;
        private int index;
        private boolean open = true;

        ByteArrayReadChannel(byte[] data) {
            this.data = Arrays.copyOf(data, data.length);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) {
                throw new ClosedChannelException();
            }
            if (index >= data.length) {
                return -1;
            }
            int toCopy = Math.min(dst.remaining(), data.length - index);
            dst.put(data, index, toCopy);
            index += toCopy;
            return toCopy;
        }

        @Override
        public void seek(long position) throws IOException {
            if (position < 0 || position > data.length) {
                throw new IOException("invalid seek");
            }
            index = (int) position;
        }

        @Override
        public void setChunkSize(int chunkSize) {
            // no-op for tests
        }

        @Override
        public com.google.cloud.RestorableState<ReadChannel> capture() {
            throw new UnsupportedOperationException("Not needed for tests.");
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

    static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS)) {
            throw new AssertionError("Timed out waiting for latch");
        }
    }
}
