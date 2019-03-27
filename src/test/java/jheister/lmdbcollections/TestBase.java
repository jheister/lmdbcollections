package jheister.lmdbcollections;

import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TestBase {
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected static <T> List<T> collect(Stream<T> stream) {
        try (Stream<T> s = stream) {
            return s.collect(Collectors.toList());
        }
    }

    protected LmdbStorageEnvironment createEnv() {
        return createEnv(1024 * 1024 * 20);
    }

    protected LmdbStorageEnvironment createEnv(long maxSize) {
        try {
            return LmdbStorageEnvironment.create(tmp.newFolder(), 3, maxSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
