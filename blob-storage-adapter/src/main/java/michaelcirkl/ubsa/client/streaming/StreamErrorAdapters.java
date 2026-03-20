package michaelcirkl.ubsa.client.streaming;

import michaelcirkl.ubsa.client.exception.UbsaException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class StreamErrorAdapters {
    private StreamErrorAdapters() {
    }

    public static Throwable unwrapCompletionException(Throwable error) {
        if (error instanceof CompletionException completionException && completionException.getCause() != null) {
            return completionException.getCause();
        }
        return error;
    }

    public static <T, E extends RuntimeException> CompletableFuture<T> wrapUbsaFuture(
            CompletableFuture<T> future,
            String message,
            Class<E> nativeExceptionType
    ) {
        return future.handle((result, error) -> {
            if (error == null) {
                return result;
            }
            Throwable cause = unwrapCompletionException(error);
            throw toCompletionException(message, cause, nativeExceptionType);
        });
    }

    public static <E extends RuntimeException> CompletionException toCompletionException(
            String message,
            Throwable cause,
            Class<E> nativeExceptionType
    ) {
        if (cause instanceof UbsaException ubsaException) {
            return new CompletionException(ubsaException);
        }
        if (nativeExceptionType.isInstance(cause)) {
            return new CompletionException(new UbsaException(message, nativeExceptionType.cast(cause)));
        }
        return new CompletionException(cause);
    }
}
