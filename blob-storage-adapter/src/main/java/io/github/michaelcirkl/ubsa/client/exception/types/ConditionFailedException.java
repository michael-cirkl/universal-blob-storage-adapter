package io.github.michaelcirkl.ubsa.client.exception.types;

import io.github.michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when a conditional request fails.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code ConditionNotMet}, {@code AppendPositionConditionNotMet},
 *   {@code MaxBlobSizeConditionNotMet}, {@code SequenceNumberConditionNotMet},
 *   {@code SourceConditionNotMet}, {@code TargetConditionNotMet}</li>
 *   <li>AWS: {@code PreconditionFailed}</li>
 * </ul>
 */
public class ConditionFailedException extends UbsaException {
    public ConditionFailedException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
