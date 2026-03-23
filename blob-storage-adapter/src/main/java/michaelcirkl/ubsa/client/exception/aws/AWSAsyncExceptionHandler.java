package michaelcirkl.ubsa.client.exception.aws;

import michaelcirkl.ubsa.client.exception.UbsaException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class AWSAsyncExceptionHandler {
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

    public UbsaException wrap(S3Exception error) {
        return new UbsaException(error.getMessage(), error, error.statusCode());
    }

    public RuntimeException propagate(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof UbsaException ubsaException) {
            return ubsaException;
        }
        if (cause instanceof S3Exception s3Exception) {
            return wrap(s3Exception);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error severeError) {
            throw severeError;
        }
        return new CompletionException(cause);
    }

    public boolean isNotFound(S3Exception error) {
        if (error instanceof NoSuchBucketException || error instanceof NoSuchKeyException) {
            return true;
        }
        return error.statusCode() == 404;
    }
}
