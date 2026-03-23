package michaelcirkl.ubsa.client.async;

import michaelcirkl.ubsa.client.streaming.FlowPublisherBridge;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FlowPublisherBridgeTest {
    @Test
    void flowToReactivePublisherPropagatesRequestAndCancel() {
        AtomicLong requested = new AtomicLong();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        Flow.Publisher<Integer> flowPublisher = subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                requested.addAndGet(n);
            }

            @Override
            public void cancel() {
                cancelled.set(true);
            }
        });

        org.reactivestreams.Publisher<Integer> reactivePublisher = FlowPublisherBridge.toReactivePublisher(flowPublisher);
        reactivePublisher.subscribe(new org.reactivestreams.Subscriber<>() {
            @Override
            public void onSubscribe(org.reactivestreams.Subscription subscription) {
                subscription.request(5);
                subscription.cancel();
            }

            @Override
            public void onNext(Integer integer) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals(5L, requested.get());
        assertTrue(cancelled.get());
    }

    @Test
    void reactiveToFlowPublisherPropagatesRequestAndCancel() {
        AtomicLong requested = new AtomicLong();
        AtomicBoolean cancelled = new AtomicBoolean(false);

        org.reactivestreams.Publisher<Integer> reactivePublisher = subscriber -> subscriber.onSubscribe(new org.reactivestreams.Subscription() {
            @Override
            public void request(long n) {
                requested.addAndGet(n);
            }

            @Override
            public void cancel() {
                cancelled.set(true);
            }
        });

        Flow.Publisher<Integer> flowPublisher = FlowPublisherBridge.toFlowPublisher(reactivePublisher);
        flowPublisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(3);
                subscription.cancel();
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals(3L, requested.get());
        assertTrue(cancelled.get());
    }

    @Test
    void reactiveToFlowPublisherPropagatesErrorAndCompletion() {
        RuntimeException failure = new RuntimeException("boom");
        AtomicReference<Throwable> observedError = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        org.reactivestreams.Publisher<Integer> errorPublisher = subscriber -> {
            subscriber.onSubscribe(new org.reactivestreams.Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.onError(failure);
        };

        FlowPublisherBridge.toFlowPublisher(errorPublisher).subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                observedError.set(throwable);
            }

            @Override
            public void onComplete() {
            }
        });

        org.reactivestreams.Publisher<Integer> completePublisher = subscriber -> {
            subscriber.onSubscribe(new org.reactivestreams.Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.onComplete();
        };

        FlowPublisherBridge.toFlowPublisher(completePublisher).subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });

        assertSame(failure, observedError.get());
        assertTrue(completed.get());
    }

    @Test
    void bridgeRejectsNonPositiveDemand() {
        AtomicBoolean reactiveCancelled = new AtomicBoolean(false);
        AtomicReference<Throwable> flowError = new AtomicReference<>();
        FlowPublisherBridge.<Integer>toFlowPublisher(subscriber -> subscriber.onSubscribe(new org.reactivestreams.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
                reactiveCancelled.set(true);
            }
        })).subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(0);
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                flowError.set(throwable);
            }

            @Override
            public void onComplete() {
            }
        });
        assertTrue(reactiveCancelled.get());
        assertNotNull(flowError.get());
        assertTrue(flowError.get() instanceof IllegalArgumentException);

        AtomicBoolean flowCancelled = new AtomicBoolean(false);
        AtomicReference<Throwable> reactiveError = new AtomicReference<>();
        FlowPublisherBridge.<Integer>toReactivePublisher(subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
                flowCancelled.set(true);
            }
        })).subscribe(new org.reactivestreams.Subscriber<>() {
            @Override
            public void onSubscribe(org.reactivestreams.Subscription subscription) {
                subscription.request(-1);
            }

            @Override
            public void onNext(Integer integer) {
            }

            @Override
            public void onError(Throwable t) {
                reactiveError.set(t);
            }

            @Override
            public void onComplete() {
            }
        });
        assertTrue(flowCancelled.get());
        assertNotNull(reactiveError.get());
        assertTrue(reactiveError.get() instanceof IllegalArgumentException);
    }

    @Test
    void mapErrorsTransformsPublisherFailures() {
        RuntimeException failure = new RuntimeException("boom");
        IllegalStateException mapped = new IllegalStateException("mapped", failure);
        AtomicReference<Throwable> observedError = new AtomicReference<>();

        FlowPublisherBridge.mapErrors((Flow.Publisher<Integer>) subscriber -> {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.onError(failure);
        }, error -> mapped).subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                observedError.set(throwable);
            }

            @Override
            public void onComplete() {
            }
        });

        assertSame(mapped, observedError.get());
    }
}
