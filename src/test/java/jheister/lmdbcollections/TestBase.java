package jheister.lmdbcollections;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TestBase {
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    protected static <T> List<T> collect(Stream<T> stream) {
        try (Stream<T> s = stream) {
            return s.collect(Collectors.toList());
        }
    }

    protected LmdbStorageEnvironment createEnv() {
        try {
            return LmdbStorageEnvironment.create(tmp.newFolder(), 1024 * 1024 * 20);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
