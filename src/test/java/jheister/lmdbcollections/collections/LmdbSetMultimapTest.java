package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment;
import jheister.lmdbcollections.TestBase;
import jheister.lmdbcollections.Transaction;
import org.junit.Test;

import static jheister.lmdbcollections.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;

public class LmdbSetMultimapTest extends TestBase {
    @Test public void
    can_add_values_to_keys() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "k1", "Hello");
                map.put(txn, "k1", "World");
                map.put(txn, "k2", "Hi");
                map.put(txn, "k2", "There");

                assertThat(collect(map.get(txn, "k1")), contains("Hello", "World"));
                assertThat(collect(map.get(txn, "k2")), contains("Hi", "There"));
            }
        }
    }

    @Test public void
    can_remove_values() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "k1", "Hello");
                map.put(txn, "k1", "World");
                map.put(txn, "k2", "Hi");
                map.put(txn, "k2", "There");

                map.remove(txn, "k1", "World");
                map.remove(txn, "k1", "absent");

                assertThat(collect(map.get(txn, "k1")), contains("Hello"));
                assertThat(collect(map.get(txn, "k2")), contains("Hi", "There"));
            }
        }
    }

    @Test public void
    does_not_contain_duplicates() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "k1", "Hello");
                map.put(txn, "k1", "Hello");
                map.put(txn, "k1", "Hello");

                assertThat(collect(map.get(txn, "k1")), contains("Hello"));
            }
        }
    }

    @Test public void
    get_on_missing_key_is_empty_stream() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put(txn, "k1", "Hello");

                assertThat(collect(map.get(txn, "another")), emptyIterable());
            }
        }
    }

    //todo: test for max size of values in set
}