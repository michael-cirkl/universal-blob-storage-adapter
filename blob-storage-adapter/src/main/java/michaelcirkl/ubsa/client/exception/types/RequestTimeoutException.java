package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class RequestTimeoutException extends UbsaException {
    public RequestTimeoutException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
