package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment;
import jheister.lmdbcollections.TestBase;
import jheister.lmdbcollections.Transaction;
import org.junit.Test;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static jheister.lmdbcollections.codec.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class LmdbSetTest extends TestBase {
    @Test public void
    can_add_elements() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSet<String> set = env.set("test", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set.add("Hello");
                set.add("World");
                set.add("I");
                set.add("am");
                set.add("a");
                set.add("Set!");

                List<String> values = new ArrayList<>();
                set.forEach(values::add);
                assertThat(values, contains(
                        "Hello",
                        "I",
                        "Set!",
                        "World",
                        "a",
                        "am"
                ));
            }
        }
    }

    @Test public void
    can_clear_all_values() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSet<String> set = env.set("test", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set.add("Hello");
                set.add("World");
                set.add("I");

                set.clear();
                set.add("am");
                set.add("a");
                set.add("Set!");

                List<String> values = new ArrayList<>();
                set.forEach(values::add);
                assertThat(values, contains(
                        "Set!",
                        "a",
                        "am"
                ));
            }
        }
    }

    @Test public void
    can_check_for_presence_of_value() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSet<String> set = env.set("test", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set.add("Hello");
                set.add("World");
                set.add("I");

                assertThat(set.contains("World"), is(true));
                assertThat(set.contains("Set!"), is(false));
            }
        }
    }

    @Test public void
    when_key_is_larger_than_511B_fails() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSet<String> set = env.set("test", STRING_CODEC);

            String maxValue = IntStream.range(0, 511).mapToObj(k -> "A").collect(Collectors.joining());

            try (Transaction txn = env.txnWrite()) {
                set.add(maxValue);
                List<String> values = new ArrayList<>();
                set.forEach(values::add);
                assertThat(values, contains(maxValue));

                thrown.expect(BufferOverflowException.class);
                set.add(maxValue + "A");
            }
        }
    }
}
