package michaelcirkl.ubsa.client.exception.gcp;

import com.google.cloud.storage.StorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class GCPAsyncExceptionHandler {
    public <T> CompletableFuture<T> handle(CompletableFuture<T> future) {
        return future.handle((result, error) -> {
            if (error == null) {
                return result;
            }
            throw propagate(error);
        });
    }

    public Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException completionException && completionException.getCause() != null) {
            return completionException.getCause();
        }
        return error;
    }

    public UbsaException wrap(StorageException error) {
        return new UbsaException(error.getMessage(), error, error.getCode());
    }

    public UbsaException wrap(IOException error) {
        return new UbsaException(error.getMessage(), error);
    }

    public RuntimeException propagate(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof UbsaException ubsaException) {
            return ubsaException;
        }
        if (cause instanceof StorageException storageException) {
            return wrap(storageException);
        }
        if (cause instanceof IOException ioException) {
            return wrap(ioException);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error severeError) {
            throw severeError;
        }
        return new CompletionException(cause);
    }

    public boolean isNotFound(StorageException error) {
        return error.getCode() == 404;
    }
}
