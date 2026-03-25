package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class BucketNotEmptyException extends UbsaException {
    public BucketNotEmptyException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
