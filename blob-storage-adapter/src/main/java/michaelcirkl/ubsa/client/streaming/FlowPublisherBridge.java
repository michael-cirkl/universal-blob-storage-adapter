package michaelcirkl.ubsa.client.streaming;

import java.util.concurrent.Flow;

public final class FlowPublisherBridge {
    private FlowPublisherBridge() {
    }

    public static <T> Flow.Publisher<T> toFlowPublisher(org.reactivestreams.Publisher<T> publisher) {
        return flowSubscriber -> {
            publisher.subscribe(new ReactiveToFlowSubscriber<>(flowSubscriber));
        };
    }

    public static <T> org.reactivestreams.Publisher<T> toReactivePublisher(Flow.Publisher<T> publisher) {
        return reactiveSubscriber -> {
            publisher.subscribe(new FlowToReactiveSubscriber<>(reactiveSubscriber));
        };
    }

    private static final class ReactiveToFlowSubscriber<T> implements org.reactivestreams.Subscriber<T> {
        private final Flow.Subscriber<? super T> downstream;

        private ReactiveToFlowSubscriber(Flow.Subscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(org.reactivestreams.Subscription subscription) {
            if (subscription == null) {
                downstream.onError(new NullPointerException("subscription must not be null"));
                return;
            }
            downstream.onSubscribe(new Flow.Subscription() {
                private volatile boolean terminated;

                @Override
                public void request(long n) {
                    if (terminated) {
                        return;
                    }
                    if (n <= 0) {
                        terminated = true;
                        subscription.cancel();
                        downstream.onError(new IllegalArgumentException("Demand must be > 0."));
                        return;
                    }
                    subscription.request(n);
                }

                @Override
                public void cancel() {
                    terminated = true;
                    subscription.cancel();
                }
            });
        }

        @Override
        public void onNext(T t) {
            downstream.onNext(t);
        }

        @Override
        public void onError(Throwable throwable) {
            downstream.onError(throwable);
        }

        @Override
        public void onComplete() {
            downstream.onComplete();
        }
    }

    private static final class FlowToReactiveSubscriber<T> implements Flow.Subscriber<T> {
        private final org.reactivestreams.Subscriber<? super T> downstream;

        private FlowToReactiveSubscriber(org.reactivestreams.Subscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (subscription == null) {
                downstream.onError(new NullPointerException("subscription must not be null"));
                return;
            }
            downstream.onSubscribe(new org.reactivestreams.Subscription() {
                private volatile boolean terminated;

                @Override
                public void request(long n) {
                    if (terminated) {
                        return;
                    }
                    if (n <= 0) {
                        terminated = true;
                        subscription.cancel();
                        downstream.onError(new IllegalArgumentException("Demand must be > 0."));
                        return;
                    }
                    subscription.request(n);
                }

                @Override
                public void cancel() {
                    terminated = true;
                    subscription.cancel();
                }
            });
        }

        @Override
        public void onNext(T item) {
            downstream.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            downstream.onError(throwable);
        }

        @Override
        public void onComplete() {
            downstream.onComplete();
        }
    }
}
