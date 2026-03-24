package michaelcirkl.ubsa.client.pagination;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.function.Function;

public final class PagedFlowPublisher<T> implements Flow.Publisher<T> {
    private final PageRequest initialRequest;
    private final Function<PageRequest, CompletableFuture<ListingPage<T>>> pageLoader;

    public PagedFlowPublisher(
            PageRequest initialRequest,
            Function<PageRequest, CompletableFuture<ListingPage<T>>> pageLoader
    ) {
        this.initialRequest = initialRequest == null ? PageRequest.firstPage() : initialRequest;
        this.pageLoader = pageLoader;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("subscriber must not be null");
        }
        subscriber.onSubscribe(new PageSubscription(subscriber));
    }

    private final class PageSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super T> downstream;
        private PageRequest nextRequest = initialRequest;
        private Iterator<T> currentItems = List.<T>of().iterator();
        private long demand;
        private boolean cancelled;
        private boolean terminated;
        private boolean loading;
        private boolean draining;
        private boolean completeAfterItems;

        private PageSubscription(Flow.Subscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                fail(new IllegalArgumentException("Demand must be > 0."));
                return;
            }
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                demand = saturatedAdd(demand, n);
                if (draining) {
                    return;
                }
                draining = true;
            }
            drain();
        }

        @Override
        public void cancel() {
            synchronized (this) {
                cancelled = true;
            }
        }

        private void drain() {
            while (true) {
                T itemToEmit = null;
                boolean shouldComplete = false;
                PageRequest requestToLoad = null;

                synchronized (this) {
                    if (cancelled || terminated) {
                        draining = false;
                        return;
                    }
                    if (demand > 0 && currentItems.hasNext()) {
                        itemToEmit = currentItems.next();
                        demand--;
                    } else if (demand > 0 && completeAfterItems) {
                        terminated = true;
                        shouldComplete = true;
                    } else if (demand > 0 && !loading) {
                        loading = true;
                        requestToLoad = nextRequest;
                    } else {
                        draining = false;
                        return;
                    }
                }

                if (itemToEmit != null) {
                    downstream.onNext(itemToEmit);
                    continue;
                }

                if (shouldComplete) {
                    downstream.onComplete();
                    return;
                }

                synchronized (this) {
                    if (cancelled || terminated) {
                        draining = false;
                        return;
                    }
                    draining = false;
                }
                PageRequest capturedRequest = requestToLoad;
                pageLoader.apply(capturedRequest).whenComplete((page, error) -> {
                    if (error != null) {
                        fail(unwrap(error));
                        return;
                    }
                    acceptPage(page == null ? ListingPage.of(List.of(), null) : page, capturedRequest);
                });
                return;
            }
        }

        private void acceptPage(ListingPage<T> page, PageRequest requestUsed) {
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                loading = false;
                currentItems = page.iterator();
                if (page.hasNextPage()) {
                    nextRequest = PageRequest.builder()
                            .pageSize(requestUsed.getPageSize())
                            .continuationToken(page.getNextContinuationToken())
                            .build();
                    completeAfterItems = false;
                } else {
                    completeAfterItems = true;
                }
                if (draining) {
                    return;
                }
                draining = true;
            }
            drain();
        }

        private void fail(Throwable throwable) {
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                cancelled = true;
                terminated = true;
            }
            downstream.onError(throwable);
        }

        private long saturatedAdd(long left, long right) {
            long result = left + right;
            if (result < 0) {
                return Long.MAX_VALUE;
            }
            return result;
        }
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            if (current.getCause() == null) {
                break;
            }
            current = current.getCause();
        }
        return current;
    }
}
