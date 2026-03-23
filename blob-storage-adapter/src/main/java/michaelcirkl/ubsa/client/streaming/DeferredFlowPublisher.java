package michaelcirkl.ubsa.client.streaming;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Function;

public final class DeferredFlowPublisher<T> implements Flow.Publisher<T> {
    private final CompletableFuture<? extends Flow.Publisher<T>> publisherFuture;
    private final Function<Throwable, RuntimeException> errorMapper;

    public DeferredFlowPublisher(
            CompletableFuture<? extends Flow.Publisher<T>> publisherFuture,
            Function<Throwable, RuntimeException> errorMapper
    ) {
        this.publisherFuture = publisherFuture;
        this.errorMapper = errorMapper;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        DeferredSubscription subscription = new DeferredSubscription(subscriber);
        subscriber.onSubscribe(subscription);
        publisherFuture.whenComplete((publisher, error) -> {
            if (error != null) {
                subscription.fail(errorMapper.apply(error));
                return;
            }
            if (subscription.isCancelled()) {
                return;
            }
            try {
                publisher.subscribe(new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription upstream) {
                        subscription.attach(upstream);
                    }

                    @Override
                    public void onNext(T item) {
                        if (!subscription.isCancelled()) {
                            subscriber.onNext(item);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        subscription.fail(errorMapper.apply(throwable));
                    }

                    @Override
                    public void onComplete() {
                        subscription.complete();
                    }
                });
            } catch (Throwable throwable) {
                subscription.fail(errorMapper.apply(throwable));
            }
        });
    }

    private final class DeferredSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super T> downstream;
        private Flow.Subscription upstream;
        private long pendingDemand;
        private boolean cancelled;
        private boolean terminated;

        private DeferredSubscription(Flow.Subscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void request(long n) {
            Flow.Subscription currentUpstream;
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                if (n <= 0) {
                    cancelled = true;
                    terminated = true;
                    currentUpstream = upstream;
                } else if (upstream == null) {
                    pendingDemand = saturatedAdd(pendingDemand, n);
                    return;
                } else {
                    currentUpstream = upstream;
                }
            }
            if (n <= 0) {
                if (currentUpstream != null) {
                    currentUpstream.cancel();
                }
                downstream.onError(new IllegalArgumentException("Demand must be > 0."));
                return;
            }
            currentUpstream.request(n);
        }

        @Override
        public void cancel() {
            Flow.Subscription currentUpstream;
            synchronized (this) {
                if (cancelled) {
                    return;
                }
                cancelled = true;
                currentUpstream = upstream;
            }
            if (currentUpstream != null) {
                currentUpstream.cancel();
            }
        }

        private synchronized boolean isCancelled() {
            return cancelled;
        }

        private void attach(Flow.Subscription upstream) {
            long demandToRequest;
            synchronized (this) {
                if (cancelled || terminated) {
                    upstream.cancel();
                    return;
                }
                this.upstream = upstream;
                demandToRequest = pendingDemand;
                pendingDemand = 0;
            }
            if (demandToRequest > 0) {
                upstream.request(demandToRequest);
            }
        }

        private void fail(Throwable throwable) {
            Flow.Subscription currentUpstream;
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                terminated = true;
                currentUpstream = upstream;
            }
            if (currentUpstream != null) {
                currentUpstream.cancel();
            }
            downstream.onError(throwable);
        }

        private void complete() {
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                terminated = true;
            }
            downstream.onComplete();
        }

        private long saturatedAdd(long left, long right) {
            long result = left + right;
            if (result < 0) {
                return Long.MAX_VALUE;
            }
            return result;
        }
    }
}
