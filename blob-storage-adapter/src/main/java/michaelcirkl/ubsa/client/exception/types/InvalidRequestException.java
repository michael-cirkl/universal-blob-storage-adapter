package michaelcirkl.ubsa.client.exception.types;

import michaelcirkl.ubsa.client.exception.UbsaException;

/**
 * Thrown when the request is malformed or contains invalid parameters for the target provider.
 *
 * <p>UBSA maps the following provider error codes to this exception:
 * <ul>
 *   <li>Azure: {@code ConditionHeadersNotSupported}, {@code InvalidBlobOrBlock}, {@code InvalidBlobTier},
 *   {@code InvalidBlobType}, {@code InvalidBlockId}, {@code InvalidBlockList}, {@code InvalidHeaderValue},
 *   {@code InvalidHttpVerb}, {@code InvalidInput}, {@code InvalidMd5}, {@code InvalidMetadata},
 *   {@code InvalidOperation}, {@code InvalidPageRange}, {@code InvalidQueryParameterValue},
 *   {@code InvalidRange}, {@code InvalidResourceName}, {@code InvalidSourceBlobType},
 *   {@code InvalidSourceBlobUrl}, {@code InvalidUri}, {@code InvalidVersionForPageBlobOperation},
 *   {@code InvalidXmlDocument}, {@code InvalidXmlNodeValue}, {@code MultipleConditionHeadersNotSupported},
 *   {@code RequestBodyTooLarge}, {@code RequestUrlFailedToParse}, {@code ResourceTypeMismatch}</li>
 *   <li>AWS: {@code InvalidArgument}, {@code InvalidBucketName}, {@code InvalidRange},
 *   {@code InvalidDigest}, {@code BadDigest}, {@code InvalidRequest}, {@code InvalidStorageClass},
 *   {@code InvalidURI}</li>
 * </ul>
 */
public class InvalidRequestException extends UbsaException {
    public InvalidRequestException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException, statusCode);
    }
}
