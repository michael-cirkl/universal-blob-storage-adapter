package michaelcirkl.ubsa.client.streaming;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;

public final class GcpReadChannelFlowPublisher implements Flow.Publisher<ByteBuffer> {
    private final Storage storage;
    private final BlobId blobId;
    private final Executor executor;

    public GcpReadChannelFlowPublisher(Storage storage, BlobId blobId, Executor executor) {
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        this.blobId = Objects.requireNonNull(blobId, "blobId must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber must not be null");
        subscriber.onSubscribe(new Flow.Subscription() {
            private final Object demandLock = new Object();
            private boolean started;
            private boolean cancelled;
            private long demand;

            @Override
            public void request(long n) {
                if (cancelled) {
                    return;
                }
                if (n <= 0) {
                    cancelled = true;
                    subscriber.onError(new IllegalArgumentException("Demand must be > 0."));
                    return;
                }
                synchronized (demandLock) {
                    demand = saturatedAdd(demand, n);
                    if (!started) {
                        started = true;
                        executor.execute(this::drain);
                    }
                    demandLock.notifyAll();
                }
            }

            @Override
            public void cancel() {
                synchronized (demandLock) {
                    cancelled = true;
                    demandLock.notifyAll();
                }
            }

            private void drain() {
                try (ReadChannel readChannel = storage.reader(blobId)) {
                    byte[] chunk = new byte[8192];
                    while (true) {
                        synchronized (demandLock) {
                            while (demand <= 0 && !cancelled) {
                                demandLock.wait();
                            }
                            if (cancelled) {
                                return;
                            }
                            demand--;
                        }
                        ByteBuffer buffer = ByteBuffer.wrap(chunk);
                        int read = readChannel.read(buffer);
                        if (read < 0) {
                            subscriber.onComplete();
                            return;
                        }
                        if (read == 0) {
                            synchronized (demandLock) {
                                demand++;
                            }
                            continue;
                        }
                        byte[] emission = new byte[read];
                        System.arraycopy(chunk, 0, emission, 0, read);
                        subscriber.onNext(ByteBuffer.wrap(emission));
                    }
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    if (!cancelled) {
                        subscriber.onError(new CompletionException("Interrupted while streaming blob content from GCS.", interruptedException));
                    }
                } catch (Throwable error) {
                    if (!cancelled) {
                        subscriber.onError(error);
                    }
                }
            }
        });
    }

    private static long saturatedAdd(long left, long right) {
        long result = left + right;
        if (result < 0) {
            return Long.MAX_VALUE;
        }
        return result;
    }
}
