package michaelcirkl.ubsa.client.pagination;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PagedFlowPublisherTest {
    @Test
    void completesAfterEmittingFinalItemWithoutExtraDemand() throws Exception {
        CompletableFuture<Void> completed = new CompletableFuture<>();
        AtomicReference<Throwable> observedError = new AtomicReference<>();
        List<Integer> emittedItems = new ArrayList<>();

        PagedFlowPublisher<Integer> publisher = new PagedFlowPublisher<>(
                PageRequest.builder().pageSize(1).build(),
                request -> CompletableFuture.completedFuture(ListingPage.of(List.of(1), null))
        );

        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(Integer item) {
                emittedItems.add(item);
            }

            @Override
            public void onError(Throwable throwable) {
                observedError.set(throwable);
                completed.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                completed.complete(null);
            }
        });

        completed.get(5, TimeUnit.SECONDS);

        assertEquals(List.of(1), emittedItems);
        assertNull(observedError.get());
    }

    @Test
    void synchronousPageLoaderFailureTerminatesSubscriptionWithOnError() {
        RuntimeException failure = new RuntimeException("boom");
        AtomicReference<Throwable> observedError = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicBoolean emittedItem = new AtomicBoolean(false);
        AtomicBoolean requestReturned = new AtomicBoolean(false);

        PagedFlowPublisher<Integer> publisher = new PagedFlowPublisher<>(
                PageRequest.builder().pageSize(1).build(),
                request -> {
                    throw failure;
                }
        );

        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                assertDoesNotThrow(() -> subscription.request(1));
                requestReturned.set(true);
            }

            @Override
            public void onNext(Integer item) {
                emittedItem.set(true);
            }

            @Override
            public void onError(Throwable throwable) {
                observedError.set(throwable);
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });

        assertTrue(requestReturned.get());
        assertSame(failure, observedError.get());
        assertFalse(emittedItem.get());
        assertFalse(completed.get());
    }

    @Test
    void cancelCancelsInFlightPageLoad() {
        CompletableFuture<ListingPage<Integer>> pageFuture = new CompletableFuture<>();
        AtomicReference<Flow.Subscription> subscriptionRef = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Throwable> observedError = new AtomicReference<>();

        PagedFlowPublisher<Integer> publisher = new PagedFlowPublisher<>(
                PageRequest.builder().pageSize(1).build(),
                request -> pageFuture
        );

        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscriptionRef.set(subscription);
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
                completed.set(true);
            }
        });

        subscriptionRef.get().cancel();

        assertTrue(pageFuture.isCancelled());
        assertFalse(completed.get());
        assertNull(observedError.get());
    }
}
