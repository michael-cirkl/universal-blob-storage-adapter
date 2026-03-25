package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class BucketNotFoundException extends UbsaException {
    public BucketNotFoundException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
