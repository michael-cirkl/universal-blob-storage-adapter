package support;

import michaelcirkl.ubsa.Provider;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.stream.Stream;

public final class SyncProviderFixtureArgumentsProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
        return Stream.of(Provider.AWS, Provider.Azure, Provider.GCP)
                .map(SyncProviderFixture::create)
                .map(Arguments::of);
    }
}
