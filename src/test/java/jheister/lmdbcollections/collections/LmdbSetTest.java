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

import static jheister.lmdbcollections.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class LmdbSetTest extends TestBase {
    @Test public void
    can_add_elements() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSet<String> set = env.createSet("test", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set.add(txn, "Hello");
                set.add(txn, "World");
                set.add(txn, "I");
                set.add(txn, "am");
                set.add(txn, "a");
                set.add(txn, "Set!");

                List<String> values = new ArrayList<>();
                set.forEach(txn, values::add);
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
            LmdbSet<String> set = env.createSet("test", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set.add(txn, "Hello");
                set.add(txn, "World");
                set.add(txn, "I");

                set.clear(txn);
                set.add(txn, "am");
                set.add(txn, "a");
                set.add(txn, "Set!");

                List<String> values = new ArrayList<>();
                set.forEach(txn, values::add);
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
            LmdbSet<String> set = env.createSet("test", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set.add(txn, "Hello");
                set.add(txn, "World");
                set.add(txn, "I");

                assertThat(set.contains(txn, "World"), is(true));
                assertThat(set.contains(txn, "Set!"), is(false));
            }
        }
    }

    @Test public void
    when_key_is_larger_than_511B_fails() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSet<String> set = env.createSet("test", STRING_CODEC);

            String maxValue = IntStream.range(0, 511).mapToObj(k -> "A").collect(Collectors.joining());

            try (Transaction txn = env.txnWrite()) {
                set.add(txn, maxValue);
                List<String> values = new ArrayList<>();
                set.forEach(txn, values::add);
                assertThat(values, contains(maxValue));

                thrown.expect(BufferOverflowException.class);
                set.add(txn, maxValue + "A");
            }
        }
    }
}
