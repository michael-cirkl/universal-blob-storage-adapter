package support;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AsyncTestSupport {
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);

    private AsyncTestSupport() {
    }

    public static <T> T await(CompletableFuture<T> future) {
        if (future == null) {
            throw new IllegalArgumentException("Future must not be null.");
        }
        try {
            return future.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for async test result.", error);
        } catch (TimeoutException error) {
            throw new IllegalStateException("Timed out while waiting for async test result.", error);
        } catch (ExecutionException error) {
            throw propagate(unwrap(error));
        }
    }

    public static byte[] readAllBytes(Flow.Publisher<ByteBuffer> publisher) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (ByteBuffer buffer : collectItems(publisher)) {
            ByteBuffer copy = buffer.asReadOnlyBuffer();
            byte[] chunk = new byte[copy.remaining()];
            copy.get(chunk);
            output.write(chunk, 0, chunk.length);
        }
        return output.toByteArray();
    }

    public static <T> List<T> collectItems(Flow.Publisher<T> publisher) {
        if (publisher == null) {
            throw new IllegalArgumentException("Publisher must not be null.");
        }

        CollectingSubscriber<T> subscriber = new CollectingSubscriber<>();
        publisher.subscribe(subscriber);
        return await(subscriber.result());
    }

    public static Flow.Publisher<ByteBuffer> publisherOf(byte[] content) {
        byte[] payload = content == null ? new byte[0] : Arrays.copyOf(content, content.length);
        return subscriber -> {
            if (subscriber == null) {
                throw new NullPointerException("subscriber must not be null");
            }

            subscriber.onSubscribe(new Flow.Subscription() {
                private boolean emitted;
                private boolean cancelled;

                @Override
                public void request(long n) {
                    if (cancelled || emitted) {
                        return;
                    }
                    if (n <= 0) {
                        cancelled = true;
                        subscriber.onError(new IllegalArgumentException("Demand must be > 0."));
                        return;
                    }

                    emitted = true;
                    if (payload.length > 0) {
                        subscriber.onNext(ByteBuffer.wrap(Arrays.copyOf(payload, payload.length)));
                    }
                    if (!cancelled) {
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    cancelled = true;
                }
            });
        };
    }

    private static RuntimeException propagate(Throwable throwable) {
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new IllegalStateException("Async test operation failed.", throwable);
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            if (current.getCause() == null) {
                break;
            }
            current = current.getCause();
        }
        return current;
    }

    private static final class CollectingSubscriber<T> implements Flow.Subscriber<T> {
        private final CompletableFuture<List<T>> result = new CompletableFuture<>();
        private final List<T> items = new ArrayList<>();

        private CompletableFuture<List<T>> result() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (subscription == null) {
                result.completeExceptionally(new NullPointerException("subscription must not be null"));
                return;
            }
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            items.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            result.complete(List.copyOf(items));
        }
    }
}
