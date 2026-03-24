package michaelcirkl.ubsa.client.exception;

import com.google.cloud.storage.StorageException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class GCPExceptionHandler {
    public <T> T handle(IOSupplier<T> action) {
        try {
            return action.get();
        } catch (StorageException error) {
            throw new UbsaException(error.getMessage(), error, error.getCode());
        } catch (IOException error) {
            throw new UbsaException(error.getMessage(), error);
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

    public UbsaException wrap(StorageException error) {
        return new UbsaException(error.getMessage(), error, error.getCode());
    }

    public UbsaException wrap(IOException error) {
        return new UbsaException(error.getMessage(), error);
    }

    public void closeQuietly(IORunnable action) {
        try {
            action.run();
        } catch (IOException ignored) {

        }
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

    public interface IOSupplier<T> { // basically just Supplier<T> (function with no args), but can throw IOException.
        T get() throws IOException;
    }

    public interface IORunnable {
        void run() throws IOException;
    }
}
