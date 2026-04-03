package support;

import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AsyncTestSupport {
    private static final long TIMEOUT_SECONDS = 30L;

    private AsyncTestSupport() {
    }

    public static <T> T await(CompletableFuture<T> future) {
        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
    }

    public static <T extends Throwable> T assertFutureThrows(Class<T> expectedType, CompletableFuture<?> future) {
        CompletionException error = Assertions.assertThrows(CompletionException.class, () -> await(future));
        return Assertions.assertInstanceOf(expectedType, error.getCause());
    }

    public static byte[] awaitBytes(Flow.Publisher<ByteBuffer> publisher) {
        List<ByteBuffer> chunks = awaitItems(publisher);
        int totalBytes = chunks.stream().mapToInt(ByteBuffer::remaining).sum();
        byte[] bytes = new byte[totalBytes];
        int offset = 0;
        for (ByteBuffer chunk : chunks) {
            ByteBuffer copy = chunk.asReadOnlyBuffer();
            int length = copy.remaining();
            copy.get(bytes, offset, length);
            offset += length;
        }
        return bytes;
    }

    public static <T> List<T> awaitItems(Flow.Publisher<T> publisher) {
        CollectingSubscriber<T> subscriber = new CollectingSubscriber<>();
        publisher.subscribe(subscriber);
        return subscriber.await();
    }

    public static <T extends Throwable> T assertPublisherThrows(Class<T> expectedType, Flow.Publisher<?> publisher) {
        CollectingSubscriber<Object> subscriber = new CollectingSubscriber<>();
        publisher.subscribe(subscriber);
        CompletionException error = Assertions.assertThrows(CompletionException.class, subscriber::await);
        return Assertions.assertInstanceOf(expectedType, error.getCause());
    }

    public static Flow.Publisher<ByteBuffer> publisherOf(byte[]... chunks) {
        List<ByteBuffer> buffers = Arrays.stream(chunks)
                .map(byte[]::clone)
                .map(ByteBuffer::wrap)
                .toList();

        return subscriber -> {
            Objects.requireNonNull(subscriber, "subscriber must not be null");
            subscriber.onSubscribe(new Flow.Subscription() {
                private final AtomicBoolean terminated = new AtomicBoolean(false);
                private int nextIndex;

                @Override
                public void request(long n) {
                    if (terminated.get()) {
                        return;
                    }
                    if (n <= 0) {
                        if (terminated.compareAndSet(false, true)) {
                            subscriber.onError(new IllegalArgumentException("Demand must be > 0."));
                        }
                        return;
                    }

                    long remainingDemand = n;
                    while (remainingDemand > 0 && nextIndex < buffers.size() && !terminated.get()) {
                        subscriber.onNext(buffers.get(nextIndex++).asReadOnlyBuffer());
                        remainingDemand--;
                    }

                    if (nextIndex == buffers.size() && terminated.compareAndSet(false, true)) {
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    terminated.set(true);
                }
            });
        };
    }

    private static final class CollectingSubscriber<T> implements Flow.Subscriber<T> {
        private final List<T> items = new CopyOnWriteArrayList<>();
        private final CompletableFuture<List<T>> completion = new CompletableFuture<>();

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            items.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            completion.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            completion.complete(List.copyOf(new ArrayList<>(items)));
        }

        private List<T> await() {
            return completion.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS).join();
        }
    }
}
