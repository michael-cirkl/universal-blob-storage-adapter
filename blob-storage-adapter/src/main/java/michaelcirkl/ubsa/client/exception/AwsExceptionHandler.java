package michaelcirkl.ubsa.client.exception;

import michaelcirkl.ubsa.UbsaException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.function.Supplier;

public final class AwsExceptionHandler {
    public <T> T handle(Supplier<T> action) {
        try {
            return action.get();
        } catch (S3Exception error) { // here can catch all specific S3 exception types and handle them
            throw new UbsaException(error.getMessage(), error, error.statusCode());
        }
    }
}
