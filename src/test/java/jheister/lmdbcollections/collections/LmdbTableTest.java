package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment;
import jheister.lmdbcollections.TestBase;
import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.collections.LmdbTable.Entry;
import org.junit.Test;

import static jheister.lmdbcollections.Codec.STRING_CODEC;
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
                        new Entry<>("R1", "A", "4"),
                        new Entry<>("R1", "B", "2")
                ));
                assertThat(collect(table.rowEntries(txn, "R2")), contains(
                        new Entry<>("R2", "C", "7"),
                        new Entry<>("R2", "D", "5")
                ));
                assertThat(collect(table.rowEntries(txn, "R3")), contains(
                        new Entry<>("R3", "E", "8"),
                        new Entry<>("R3", "F", "9")
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
                        new Entry<>("A", "Aa", "1"),
                        new Entry<>("A", "Ab", "2"),
                        new Entry<>("A", "Ac", "3")
                ));
                assertThat(collect(table.rowEntries(txn, "AA")), contains(
                        new Entry<>("AA", "a", "4"),
                        new Entry<>("AA", "b", "5"),
                        new Entry<>("AA", "c", "6")
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
                        new Entry<>("row1", "col1", "Hello"),
                        new Entry<>("row1", "col2", "Alternative")
                ));
            }
        }
    }

    //todo: test for max size of values in set
}