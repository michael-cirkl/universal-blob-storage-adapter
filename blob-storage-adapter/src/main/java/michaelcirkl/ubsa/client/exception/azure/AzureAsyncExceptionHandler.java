package michaelcirkl.ubsa.client.exception.azure;

import com.azure.storage.blob.models.BlobStorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class AzureAsyncExceptionHandler {
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
