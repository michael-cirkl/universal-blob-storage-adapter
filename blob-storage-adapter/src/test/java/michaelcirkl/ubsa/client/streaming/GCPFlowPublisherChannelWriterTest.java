package michaelcirkl.ubsa.client.streaming;

import michaelcirkl.ubsa.client.gcp.GCPFlowPublisherChannelWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.*;

class GCPFlowPublisherChannelWriterTest {
    @Test
    void writesPublisherContentToChannel() {
        CapturingWritableByteChannel channel = new CapturingWritableByteChannel();
        GCPFlowPublisherChannelWriter.writeFromPublisher(publisherOf("abc".getBytes(), "def".getBytes()), channel, 6);
        assertArrayEquals("abcdef".getBytes(), channel.bytes());
    }

    @Test
    void failsWhenPublisherLengthDoesNotMatch() {
        CapturingWritableByteChannel channel = new CapturingWritableByteChannel();
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GCPFlowPublisherChannelWriter.writeFromPublisher(publisherOf("ab".getBytes()), channel, 3)
        );
        assertTrue(error.getMessage().contains("Content length mismatch"));
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

        byte[] bytes() {
            return output.toByteArray();
        }
    }
}
