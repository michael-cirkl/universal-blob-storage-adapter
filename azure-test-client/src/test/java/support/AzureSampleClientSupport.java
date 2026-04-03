package support;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class AzureSampleClientSupport {
    private static final String ACCOUNT_NAME_KEY = "AZURE_STORAGE_ACCOUNT_NAME";
    private static final String ACCOUNT_KEY_KEY = "AZURE_STORAGE_ACCOUNT_KEY";
    private static final String ENV_FILE_NAME = ".env";
    private static final String ENV_EXAMPLE_FILE_NAME = ".env.example";
    private static final String AZURITE_CONNECTION_STRING_TEMPLATE =
            "DefaultEndpointsProtocol=http;"
                    + "AccountName=%s;"
                    + "AccountKey=%s;"
                    + "BlobEndpoint=http://127.0.0.1:10000/%s;";

    private AzureSampleClientSupport() {
    }

    public static BlobServiceClient createNativeSyncClient() {
        return new BlobServiceClientBuilder()
                .connectionString(connectionString())
                .buildClient();
    }

    public static BlobServiceAsyncClient createNativeAsyncClient() {
        return new BlobServiceClientBuilder()
                .connectionString(connectionString())
                .buildAsyncClient();
    }

    public static BlobStorageSyncClient createUbsaSyncClient() {
        return BlobStorageClientFactory.getSyncClient(createNativeSyncClient());
    }

    public static BlobStorageAsyncClient createUbsaAsyncClient() {
        return BlobStorageClientFactory.getAsyncClient(createNativeAsyncClient());
    }

    private static String connectionString() {
        Map<String, String> env = loadEnvFile();
        String accountName = required(env, ACCOUNT_NAME_KEY);
        String accountKey = required(env, ACCOUNT_KEY_KEY);
        return AZURITE_CONNECTION_STRING_TEMPLATE.formatted(accountName, accountKey, accountName);
    }

    private static Map<String, String> loadEnvFile() {
        Path envPath = locateEnvFile();
        List<String> lines;
        try {
            lines = Files.readAllLines(envPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + envPath + ".", e);
        }

        Map<String, String> values = new HashMap<>();
        for (String rawLine : lines) {
            String line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            int separator = line.indexOf('=');
            if (separator <= 0) {
                continue;
            }

            String key = line.substring(0, separator).trim();
            String value = stripQuotes(line.substring(separator + 1).trim());
            values.put(key, value);
        }
        return values;
    }

    private static Path locateEnvFile() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(ENV_FILE_NAME);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }

        throw new IllegalStateException(
                ENV_FILE_NAME + " was not found. There is an Azurite emulator example in " + ENV_EXAMPLE_FILE_NAME
        );
    }

    private static String required(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required key " + key + " in " + ENV_FILE_NAME + ".");
        }
        return value;
    }

    private static String stripQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }
}
