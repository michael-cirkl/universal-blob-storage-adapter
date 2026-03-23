package michaelcirkl.ubsa.client.exception;

import com.azure.storage.blob.models.BlobStorageException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

public class AzureExceptionHandler {
    public <T> T handle(Supplier<T> action) {
        try {
            return action.get();
        } catch (BlobStorageException error) { // here can catch all specific Azure exception types and handle them
            throw new UbsaException(error.getMessage(), error, error.getStatusCode());
        }
    }

    public <T> CompletableFuture<T> handleAsync(CompletableFuture<T> future) {
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

    public UbsaException wrap(BlobStorageException error) {
        return new UbsaException(error.getMessage(), error, error.getStatusCode());
    }

    public RuntimeException propagate(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof UbsaException ubsaException) {
            return ubsaException;
        }
        if (cause instanceof BlobStorageException blobStorageException) {
            return wrap(blobStorageException);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error severeError) {
            throw severeError;
        }
        return new CompletionException(cause);
    }
}
