package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

public class ConditionFailedException extends UbsaException {
    public ConditionFailedException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
