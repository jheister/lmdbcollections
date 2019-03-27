package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment;
import jheister.lmdbcollections.TestBase;
import jheister.lmdbcollections.Transaction;
import org.junit.Test;

import java.nio.BufferOverflowException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static jheister.lmdbcollections.codec.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LmdbTableTest extends TestBase {
    @Test public void
    can_put_and_get_values() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.createTable("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put(txn, "row1", "col1", "Hello");
                table.put(txn, "row1", "col2", "World");
                table.put(txn, "row2", "col1", "Hi");
                table.put(txn, "row2", "col2", "Alternate column");

                assertThat(table.get(txn, "row1", "col1"), is("Hello"));
                assertThat(table.get(txn, "row1", "col2"), is("World"));
                assertThat(table.get(txn, "row2", "col1"), is("Hi"));
                assertThat(table.get(txn, "row2", "col2"), is("Alternate column"));
            }
        }
    }

    @Test public void
    after_key_is_removed_it_returns_null() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.createTable("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put(txn, "row1", "col1", "Hello");
                table.put(txn, "row1", "col2", "World");
                table.put(txn, "row2", "col1", "Hi");
                table.put(txn, "row2", "col2", "Alternate column");

                table.remove(txn, "row1", "col2");

                assertThat(table.get(txn, "row1", "col1"), is("Hello"));
                assertThat(table.get(txn, "row1", "col2"), nullValue());
                assertThat(table.get(txn, "row2", "col1"), is("Hi"));
                assertThat(table.get(txn, "row2", "col2"), is("Alternate column"));
            }
        }
    }

    @Test public void
    lists_all_entries_in_a_row_in_order() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.createTable("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put(txn, "R1", "B", "2");
                table.put(txn, "R1", "A", "4");
                table.put(txn, "R2", "C", "7");
                table.put(txn, "R2", "D", "5");
                table.put(txn, "R3", "E", "8");
                table.put(txn, "R3", "F", "9");

                assertThat(collect(table.rowEntries(txn, "R1")), contains(
                        new Entry<>("A", "4"),
                        new Entry<>("B", "2")
                ));
                assertThat(collect(table.rowEntries(txn, "R2")), contains(
                        new Entry<>("C", "7"),
                        new Entry<>("D", "5")
                ));
                assertThat(collect(table.rowEntries(txn, "R3")), contains(
                        new Entry<>("E", "8"),
                        new Entry<>("F", "9")
                ));
            }
        }
    }

    @Test public void
    when_row_and_col_keys_combine_to_the_same_value_correct_entries_are_returned() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.createTable("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put(txn, "A", "Aa", "1");
                table.put(txn, "A", "Ab", "2");
                table.put(txn, "A", "Ac", "3");
                table.put(txn, "AA", "a", "4");
                table.put(txn, "AA", "b", "5");
                table.put(txn, "AA", "c", "6");

                assertThat(collect(table.rowEntries(txn, "A")), contains(
                        new Entry<>("Aa", "1"),
                        new Entry<>("Ab", "2"),
                        new Entry<>("Ac", "3")
                ));
                assertThat(collect(table.rowEntries(txn, "AA")), contains(
                        new Entry<>("a", "4"),
                        new Entry<>("b", "5"),
                        new Entry<>("c", "6")
                ));
            }
        }
    }

    @Test public void
    does_not_store_duplicates_on_keys() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.createTable("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put(txn, "row1", "col1", "Hello");
                table.put(txn, "row1", "col2", "World");
                table.put(txn, "row1", "col2", "Alternative");

                assertThat(collect(table.rowEntries(txn, "row1")), contains(
                        new Entry<>("col1", "Hello"),
                        new Entry<>("col2", "Alternative")
                ));
            }
        }
    }

    @Test public void
    row_and_col_key_together_have_to_be_under_507B() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> multimap = env.createTable("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            String rowKey_300 = IntStream.range(0, 300).mapToObj(k -> "A").collect(Collectors.joining());
            String colKey_207 = IntStream.range(0, 207).mapToObj(k -> "A").collect(Collectors.joining());
            String colKey_208 = IntStream.range(0, 208).mapToObj(k -> "A").collect(Collectors.joining());

            try (Transaction txn = env.txnWrite()) {
                multimap.put(txn, rowKey_300, colKey_207, "value");
                assertThat(multimap.get(txn, rowKey_300, colKey_207), is("value"));

                thrown.expect(BufferOverflowException.class);
                multimap.put(txn, rowKey_300, colKey_208, "value");
            }
        }
    }
}