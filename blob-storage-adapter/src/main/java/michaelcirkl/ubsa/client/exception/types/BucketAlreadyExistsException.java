package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class BucketAlreadyExistsException extends UbsaException {
    public BucketAlreadyExistsException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
