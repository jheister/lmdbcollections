package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment;
import jheister.lmdbcollections.TestBase;
import jheister.lmdbcollections.Transaction;
import org.junit.Test;

import static jheister.lmdbcollections.codec.Codec.INTEGER_CODEC;
import static jheister.lmdbcollections.codec.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LmdbMapTest extends TestBase {
    @Test
    public void
    can_put_and_get_values() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.map("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("k1", "Hello");
                map.put("k2", "Hi");

                assertThat(map.get("k1"), is("Hello"));
                assertThat(map.get("k2"), is("Hi"));
            }
        }
    }

    @Test public void
    after_key_is_removed_it_returns_null() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.map("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("key1", "Hello");
                map.put("key2", "World");

                map.remove("key2");

                assertThat(map.get("key1"), is("Hello"));
                assertThat(map.get("key2"), nullValue());
            }
        }
    }

    @Test public void
    entries_are_listed_in_order() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.map("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("A", "Hello");
                map.put("C", "Some");
                map.put("B", "Hi");

                assertThat(collect(map.entries()), contains(
                        new Entry<>("A", "Hello"),
                        new Entry<>("B", "Hi"),
                        new Entry<>("C", "Some")
                ));
            }
        }
    }

    @Test public void
    does_not_store_duplicates_on_keys() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.map("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("key1", "Hello");
                map.put("key1", "Alternative");

                assertThat(collect(map.entries()), contains(
                        new Entry<>("key1", "Alternative")
                ));
            }
        }
    }

    @Test public void
    int_map_is_sorted_on_byte_representation_of_keys() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<Integer, String> map = env.map("test", INTEGER_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(6, "Hello");
                map.put(3, "Alternative");
                map.put(-1, "negative");

                assertThat(collect(map.entries()), contains(
                        new Entry<>(3, "Alternative"),
                        new Entry<>(6, "Hello"),
                        new Entry<>(-1, "negative")
                ));
            }
        }
    }

    @Test
    public void
    can_test_for_presence_of_key() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.map("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("k1", "Hello");
                map.put("k2", "Hi");

                assertThat(map.containsKey("k1"), is(true));
                assertThat(map.containsKey("k2"), is(true));
                assertThat(map.containsKey("other"), is(false));
            }
        }
    }

    @Test public void
    can_clear_entire_map() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.map("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("key1", "Hello");
                map.put("key2", "World");

                map.clear();

                assertThat(map.entries().count(), is(0L));
            }
        }
    }
}