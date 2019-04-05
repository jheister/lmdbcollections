package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment;
import jheister.lmdbcollections.TestBase;
import jheister.lmdbcollections.Transaction;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static jheister.lmdbcollections.codec.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;

public class LmdbSetMultimapTest extends TestBase {
    @Test public void
    can_add_values_to_keys() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSortedSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("k1", "Hello");
                map.put("k1", "World");
                map.put("k2", "Hi");
                map.put("k2", "There");

                assertThat(collect(map.get("k1")), contains("Hello", "World"));
                assertThat(collect(map.get("k2")), contains("Hi", "There"));
            }
        }
    }

    @Test public void
    can_remove_values() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSortedSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("k1", "Hello");
                map.put("k1", "World");
                map.put("k2", "Hi");
                map.put("k2", "There");

                map.remove("k1", "World");
                map.remove("k1", "absent");

                assertThat(collect(map.get("k1")), contains("Hello"));
                assertThat(collect(map.get("k2")), contains("Hi", "There"));
            }
        }
    }

    @Test public void
    does_not_contain_duplicates() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSortedSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("k1", "Hello");
                map.put("k1", "Hello");
                map.put("k1", "Hello");

                assertThat(collect(map.get("k1")), contains("Hello"));
            }
        }
    }

    @Test public void
    get_on_missing_key_is_empty_stream() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> map = env.createSortedSetMultimap("test", STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                map.put("k1", "Hello");

                assertThat(collect(map.get("another")), emptyIterable());
            }
        }
    }

    @Test public void
    key_and_value_can_be_larger_than_lmdb_key_limit() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbSetMultimap<String, String> multimap = env.createSortedSetMultimap("test", STRING_CODEC, STRING_CODEC);

            String key_300 = IntStream.range(0, 300).mapToObj(k -> "A").collect(Collectors.joining());
            String value_208 = IntStream.range(0, 208).mapToObj(k -> "A").collect(Collectors.joining());

            try (Transaction txn = env.txnWrite()) {
                multimap.put(key_300, value_208);
            }
        }
    }
}