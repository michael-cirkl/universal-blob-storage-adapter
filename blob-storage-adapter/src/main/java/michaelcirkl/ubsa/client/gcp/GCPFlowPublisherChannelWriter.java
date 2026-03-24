package michaelcirkl.ubsa.client.gcp;

import michaelcirkl.ubsa.client.streaming.ContentLengthValidators;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class GCPFlowPublisherChannelWriter {
    private GCPFlowPublisherChannelWriter() {
    }

    public static void writeFromPublisher(Flow.Publisher<ByteBuffer> publisher, WritableByteChannel channel, long contentLength) {
        ContentLengthValidators.validateContentLength(contentLength);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        AtomicLong bytesWritten = new AtomicLong();

        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (subscription == null) {
                    errorRef.set(new NullPointerException("subscription must not be null"));
                    done.countDown();
                    return;
                }
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(ByteBuffer item) {
                try {
                    ByteBuffer source = item == null ? ByteBuffer.allocate(0) : item;
                    ByteBuffer buffer = source.slice();
                    while (buffer.hasRemaining()) {
                        bytesWritten.addAndGet(channel.write(buffer));
                    }
                    subscription.request(1);
                } catch (Throwable error) {
                    errorRef.set(error);
                    subscription.cancel();
                    done.countDown();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                errorRef.set(throwable);
                done.countDown();
            }

            @Override
            public void onComplete() {
                done.countDown();
            }
        });

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException("Interrupted while streaming content to GCS.", e);
        }

        Throwable streamError = errorRef.get();
        if (streamError != null) {
            throw new CompletionException("Failed while consuming stream content for GCS upload.", streamError);
        }
        ContentLengthValidators.ensureExactByteCount(contentLength, bytesWritten.get());
    }
}
