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

import static org.junit.jupiter.api.Assertions.*;

class ContentLengthValidatorsTest {

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
