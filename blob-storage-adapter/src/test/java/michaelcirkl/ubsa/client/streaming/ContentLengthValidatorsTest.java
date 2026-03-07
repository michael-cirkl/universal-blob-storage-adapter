package michaelcirkl.ubsa.client.streaming;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContentLengthValidatorsTest {
    @Test
    void enforcePublisherContentLengthAcceptsExactLength() throws Exception {
        Flow.Publisher<ByteBuffer> source = publisherOf("ab".getBytes(), "cd".getBytes());
        byte[] collected = collect(ContentLengthValidators.enforcePublisherContentLength(source, 4));
        assertArrayEquals("abcd".getBytes(), collected);
    }

    @Test
    void enforcePublisherContentLengthRejectsUnderflowAndOverflow() {
        Flow.Publisher<ByteBuffer> underflow = publisherOf("ab".getBytes());
        ExecutionException underflowError = assertThrows(
                ExecutionException.class,
                () -> collect(ContentLengthValidators.enforcePublisherContentLength(underflow, 3))
        );
        assertTrue(underflowError.getCause() instanceof IllegalArgumentException);
        assertTrue(underflowError.getCause().getMessage().contains("Content length mismatch"));

        Flow.Publisher<ByteBuffer> overflow = publisherOf("ab".getBytes(), "c".getBytes());
        ExecutionException overflowError = assertThrows(
                ExecutionException.class,
                () -> collect(ContentLengthValidators.enforcePublisherContentLength(overflow, 2))
        );
        assertTrue(overflowError.getCause() instanceof IllegalArgumentException);
        assertTrue(overflowError.getCause().getMessage().contains("Content length mismatch"));
    }

    @Test
    void copyInputStreamToChannelEnforcesExactLength() throws Exception {
        byte[] content = "channel-data".getBytes();
        CapturingWritableByteChannel channel = new CapturingWritableByteChannel();
        long written = ContentLengthValidators.copyInputStreamToChannel(new ByteArrayInputStream(content), channel, content.length);
        assertEquals(content.length, written);
        assertArrayEquals(content, channel.toByteArray());

        IllegalArgumentException underflow = assertThrows(
                IllegalArgumentException.class,
                () -> ContentLengthValidators.copyInputStreamToChannel(new ByteArrayInputStream("ab".getBytes()), new CapturingWritableByteChannel(), 3)
        );
        assertTrue(underflow.getMessage().contains("Content length mismatch"));

        IllegalArgumentException overflow = assertThrows(
                IllegalArgumentException.class,
                () -> ContentLengthValidators.copyInputStreamToChannel(new ByteArrayInputStream("abcd".getBytes()), new CapturingWritableByteChannel(), 3)
        );
        assertTrue(overflow.getMessage().contains("Content length mismatch"));
    }

    private static byte[] collect(Flow.Publisher<ByteBuffer> publisher) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CompletableFuture<byte[]> done = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                ByteBuffer copy = item == null ? ByteBuffer.allocate(0) : item.slice();
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

    private static Flow.Publisher<ByteBuffer> publisherOf(byte[]... chunks) {
        byte[][] copy = Arrays.stream(chunks).map(chunk -> Arrays.copyOf(chunk, chunk.length)).toArray(byte[][]::new);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean cancelled;

            @Override
            public void request(long n) {
                if (cancelled || n <= 0) {
                    return;
                }
                long demand = n;
                while (!cancelled && demand > 0 && index < copy.length) {
                    subscriber.onNext(ByteBuffer.wrap(copy[index++]));
                    demand--;
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

        byte[] toByteArray() {
            return output.toByteArray();
        }
    }
}
