package support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class TestEnvironment {
    private static final String ENV_FILE_NAME = ".env";
    private static final String ENV_EXAMPLE_FILE_NAME = ".env.example";
    private static final TestEnvironment INSTANCE = new TestEnvironment(loadValues());

    private final Map<String, String> values;

    private TestEnvironment(Map<String, String> values) {
        this.values = values;
    }

    static TestEnvironment load() {
        return INSTANCE;
    }

    String required(String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required key " + key + " in " + ENV_FILE_NAME + ".");
        }
        return value;
    }

    private static Map<String, String> loadValues() {
        Path envPath = locateEnvFile();
        List<String> lines;
        try {
            lines = Files.readAllLines(envPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + envPath + ".", e);
        }

        Map<String, String> loaded = new HashMap<>();
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
            loaded.put(key, value);
        }
        return Map.copyOf(loaded);
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
                ENV_FILE_NAME + " was not found. There is an emulator example in " + ENV_EXAMPLE_FILE_NAME + "."
        );
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
