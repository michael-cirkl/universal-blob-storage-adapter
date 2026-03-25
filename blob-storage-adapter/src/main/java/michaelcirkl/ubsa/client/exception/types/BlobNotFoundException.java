package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class BlobNotFoundException extends UbsaException {
    public BlobNotFoundException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
