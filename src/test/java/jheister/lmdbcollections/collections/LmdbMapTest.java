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
            LmdbMap<String, String> map = env.createMap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "k1", "Hello");
                map.put(txn, "k2", "Hi");

                assertThat(map.get(txn, "k1"), is("Hello"));
                assertThat(map.get(txn, "k2"), is("Hi"));
            }
        }
    }

    @Test public void
    after_key_is_removed_it_returns_null() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.createMap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "key1", "Hello");
                map.put(txn, "key2", "World");

                map.remove(txn, "key2");

                assertThat(map.get(txn, "key1"), is("Hello"));
                assertThat(map.get(txn, "key2"), nullValue());
            }
        }
    }

    @Test public void
    entries_are_listed_in_order() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<String, String> map = env.createMap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "A", "Hello");
                map.put(txn, "C", "Some");
                map.put(txn, "B", "Hi");

                assertThat(collect(map.entries(txn)), contains(
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
            LmdbMap<String, String> map = env.createMap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "key1", "Hello");
                map.put(txn, "key1", "Alternative");

                assertThat(collect(map.entries(txn)), contains(
                        new Entry<>("key1", "Alternative")
                ));
            }
        }
    }

    @Test public void
    can_have_int_string_map() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbMap<Integer, String> map = env.createMap("test", INTEGER_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, 6, "Hello");
                map.put(txn, 3, "Alternative");

                assertThat(collect(map.entries(txn)), contains(
                        new Entry<>(3, "Alternative"),
                        new Entry<>(6, "Hello")
                ));
            }
        }
    }
}