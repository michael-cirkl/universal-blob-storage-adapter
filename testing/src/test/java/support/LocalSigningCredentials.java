package support;

import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.List;
import java.util.Map;

final class LocalSigningCredentials extends Credentials implements ServiceAccountSigner {
    private final String account;
    private final PrivateKey privateKey;

    LocalSigningCredentials(String account, PrivateKey privateKey) {
        this.account = account;
        this.privateKey = privateKey;
    }

    static LocalSigningCredentials create(String projectId) {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(2048);
            KeyPair keyPair = keyPairGenerator.generateKeyPair();
            String account = "ubsa-tests@" + projectId + ".iam.gserviceaccount.com";
            return new LocalSigningCredentials(account, keyPair.getPrivate());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to create local signing credentials for the GCP emulator.", e);
        }
    }

    @Override
    public String getAuthenticationType() {
        return "UBSA-Local-ServiceAccount";
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) {
        return Map.of();
    }

    @Override
    public boolean hasRequestMetadata() {
        return false;
    }

    @Override
    public boolean hasRequestMetadataOnly() {
        return false;
    }

    @Override
    public void refresh() {
    }

    @Override
    public String getAccount() {
        return account;
    }

    @Override
    public byte[] sign(byte[] bytes) {
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(bytes);
            return signature.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(
                    "Failed to sign emulator request bytes: " + new String(bytes, StandardCharsets.UTF_8),
                    e
            );
        }
    }
}
