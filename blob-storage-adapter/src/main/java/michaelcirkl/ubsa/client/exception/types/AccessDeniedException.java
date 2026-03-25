package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class AccessDeniedException extends UbsaException {
    public AccessDeniedException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
