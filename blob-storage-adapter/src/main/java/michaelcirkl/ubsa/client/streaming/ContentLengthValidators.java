package michaelcirkl.ubsa.client.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class ContentLengthValidators {
    private static final int STREAM_BUFFER_SIZE = 8192;

    private ContentLengthValidators() {
    }

    public static void validateContentLength(long contentLength) {
        if (contentLength < 0) {
            throw new IllegalArgumentException("contentLength must be >= 0.");
        }
    }

    public static IllegalArgumentException lengthExceeded(long expectedLength) {
        return new IllegalArgumentException(
                "Content length mismatch. Expected " + expectedLength + " bytes but stream exceeded that length."
        );
    }

    public static IllegalArgumentException lengthMismatch(long expectedLength, long actualLength) {
        return new IllegalArgumentException(
                "Content length mismatch. Expected " + expectedLength + " bytes but received " + actualLength + " bytes."
        );
    }

    public static void ensureExactByteCount(long expectedLength, long actualLength) {
        if (actualLength > expectedLength) {
            throw lengthExceeded(expectedLength);
        }
        if (actualLength < expectedLength) {
            throw lengthMismatch(expectedLength, actualLength);
        }
    }

    public static long copyInputStreamToChannel(InputStream content, WritableByteChannel channel, long expectedLength) throws IOException {
        Objects.requireNonNull(content, "content must not be null");
        Objects.requireNonNull(channel, "channel must not be null");
        validateContentLength(expectedLength);

        byte[] buffer = new byte[STREAM_BUFFER_SIZE];
        long remaining = expectedLength;
        long written = 0L;

        while (remaining > 0) {
            int read = content.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (read == -1) {
                throw lengthMismatch(expectedLength, written);
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, read);
            while (byteBuffer.hasRemaining()) {
                written += channel.write(byteBuffer);
            }
            remaining -= read;
        }

        if (content.read() != -1) {
            throw lengthExceeded(expectedLength);
        }

        return written;
    }

    public static Flow.Publisher<ByteBuffer> enforcePublisherContentLength(Flow.Publisher<ByteBuffer> source, long expectedLength) {
        Objects.requireNonNull(source, "source publisher must not be null");
        validateContentLength(expectedLength);

        return downstream -> {
            Objects.requireNonNull(downstream, "subscriber must not be null");
            source.subscribe(new Flow.Subscriber<>() {
                private Flow.Subscription upstream;
                private long emittedBytes;
                private boolean done;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    if (subscription == null) {
                        done = true;
                        downstream.onError(new NullPointerException("subscription must not be null"));
                        return;
                    }
                    upstream = subscription;
                    downstream.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long n) {
                            if (done) {
                                return;
                            }
                            if (n <= 0) {
                                done = true;
                                upstream.cancel();
                                downstream.onError(new IllegalArgumentException("Demand must be > 0."));
                                return;
                            }
                            upstream.request(n);
                        }

                        @Override
                        public void cancel() {
                            done = true;
                            upstream.cancel();
                        }
                    });
                }

                @Override
                public void onNext(ByteBuffer item) {
                    if (done) {
                        return;
                    }
                    long chunkSize = item == null ? 0L : item.remaining();
                    emittedBytes += chunkSize;
                    if (emittedBytes > expectedLength) {
                        done = true;
                        upstream.cancel();
                        downstream.onError(lengthExceeded(expectedLength));
                        return;
                    }
                    downstream.onNext(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    if (done) {
                        return;
                    }
                    done = true;
                    downstream.onError(throwable);
                }

                @Override
                public void onComplete() {
                    if (done) {
                        return;
                    }
                    done = true;
                    if (emittedBytes != expectedLength) {
                        downstream.onError(lengthMismatch(expectedLength, emittedBytes));
                        return;
                    }
                    downstream.onComplete();
                }
            });
        };
    }
}
