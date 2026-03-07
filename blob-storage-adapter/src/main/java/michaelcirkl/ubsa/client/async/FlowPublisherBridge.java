package michaelcirkl.ubsa.client.async;

import java.util.Objects;
import java.util.concurrent.Flow;

public class FlowPublisherBridge {
    private FlowPublisherBridge() {
    }

    static <T> Flow.Publisher<T> toFlowPublisher(org.reactivestreams.Publisher<T> publisher) {
        Objects.requireNonNull(publisher, "publisher must not be null");
        return flowSubscriber -> publisher.subscribe(new ReactiveToFlowSubscriber<>(flowSubscriber));
    }

    static <T> org.reactivestreams.Publisher<T> toReactivePublisher(Flow.Publisher<T> publisher) {
        Objects.requireNonNull(publisher, "publisher must not be null");
        return reactiveSubscriber -> publisher.subscribe(new FlowToReactiveSubscriber<>(reactiveSubscriber));
    }

    private static final class ReactiveToFlowSubscriber<T> implements org.reactivestreams.Subscriber<T> {
        private final Flow.Subscriber<? super T> downstream;

        private ReactiveToFlowSubscriber(Flow.Subscriber<? super T> downstream) {
            this.downstream = Objects.requireNonNull(downstream, "subscriber must not be null");
        }

        @Override
        public void onSubscribe(org.reactivestreams.Subscription subscription) {
            downstream.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    subscription.request(n);
                }

                @Override
                public void cancel() {
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
            this.downstream = Objects.requireNonNull(downstream, "subscriber must not be null");
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            downstream.onSubscribe(new org.reactivestreams.Subscription() {
                @Override
                public void request(long n) {
                    subscription.request(n);
                }

                @Override
                public void cancel() {
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
