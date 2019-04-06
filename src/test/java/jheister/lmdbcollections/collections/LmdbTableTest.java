package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment;
import jheister.lmdbcollections.TestBase;
import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.collections.LmdbTable.TableEntry;
import org.junit.Test;

import java.nio.BufferOverflowException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static jheister.lmdbcollections.codec.Codec.INTEGER_CODEC;
import static jheister.lmdbcollections.codec.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LmdbTableTest extends TestBase {
    @Test public void
    can_put_and_get_values() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("row1", "col1", "Hello");
                table.put("row1", "col2", "World");
                table.put("row2", "col1", "Hi");
                table.put("row2", "col2", "Alternate column");

                assertThat(table.get("row1", "col1"), is("Hello"));
                assertThat(table.get("row1", "col2"), is("World"));
                assertThat(table.get("row2", "col1"), is("Hi"));
                assertThat(table.get("row2", "col2"), is("Alternate column"));
            }
        }
    }

    @Test public void
    after_key_is_removed_it_returns_null() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("row1", "col1", "Hello");
                table.put("row1", "col2", "World");
                table.put("row2", "col1", "Hi");
                table.put("row2", "col2", "Alternate column");

                table.remove("row1", "col2");

                assertThat(table.get("row1", "col1"), is("Hello"));
                assertThat(table.get("row1", "col2"), nullValue());
                assertThat(table.get("row2", "col1"), is("Hi"));
                assertThat(table.get("row2", "col2"), is("Alternate column"));
            }
        }
    }

    @Test public void
    lists_all_entries_in_a_row_in_order() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("R1", "B", "2");
                table.put("R1", "A", "4");
                table.put("R2", "C", "7");
                table.put("R2", "D", "5");
                table.put("R3", "E", "8");
                table.put("R3", "F", "9");

                assertThat(collect(table.rowEntries("R1")), contains(
                        new TableEntry<>("R1", "A", "4"),
                        new TableEntry<>("R1", "B", "2")
                ));
                assertThat(collect(table.rowEntries("R2")), contains(
                        new TableEntry<>("R2", "C", "7"),
                        new TableEntry<>("R2", "D", "5")
                ));
                assertThat(collect(table.rowEntries("R3")), contains(
                        new TableEntry<>("R3", "E", "8"),
                        new TableEntry<>("R3", "F", "9")
                ));
            }
        }
    }

    @Test public void
    when_row_and_col_keys_combine_to_the_same_value_correct_entries_are_returned() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("A", "Aa", "1");
                table.put("A", "Ab", "2");
                table.put("A", "Ac", "3");
                table.put("AA", "a", "4");
                table.put("AA", "b", "5");
                table.put("AA", "c", "6");

                assertThat(collect(table.rowEntries("A")), contains(
                        new TableEntry<>("A", "Aa", "1"),
                        new TableEntry<>("A", "Ab", "2"),
                        new TableEntry<>("A", "Ac", "3")
                ));
                assertThat(collect(table.rowEntries("AA")), contains(
                        new TableEntry<>("AA", "a", "4"),
                        new TableEntry<>("AA", "b", "5"),
                        new TableEntry<>("AA", "c", "6")
                ));
            }
        }
    }

    @Test public void
    does_not_store_duplicates_on_keys() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("row1", "col1", "Hello");
                table.put("row1", "col2", "World");
                table.put("row1", "col2", "Alternative");

                assertThat(collect(table.rowEntries("row1")), contains(
                        new TableEntry<>("row1", "col1", "Hello"),
                        new TableEntry<>("row1", "col2", "Alternative")
                ));
            }
        }
    }

    @Test public void
    row_and_col_key_together_have_to_be_under_507B() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> multimap = env.table("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            String rowKey_300 = IntStream.range(0, 300).mapToObj(k -> "A").collect(Collectors.joining());
            String colKey_207 = IntStream.range(0, 207).mapToObj(k -> "A").collect(Collectors.joining());
            String colKey_208 = IntStream.range(0, 208).mapToObj(k -> "A").collect(Collectors.joining());

            try (Transaction txn = env.txnWrite()) {
                multimap.put(rowKey_300, colKey_207, "value");
                assertThat(multimap.get(rowKey_300, colKey_207), is("value"));

                thrown.expect(BufferOverflowException.class);
                multimap.put(rowKey_300, colKey_208, "value");
            }
        }
    }

    @Test public void
    can_test_for_presence_of_row() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test", STRING_CODEC, STRING_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("row1", "col1", "Hello");
                table.put("row2", "col1", "Hi");

                assertThat(table.containsRow("row1"), is(true));
                assertThat(table.containsRow("row2"), is(true));
                assertThat(table.containsRow("row3"), is(false));
            }
        }
    }

    @Test public void
    when_both_row_and_col_have_specific_ordering_this_is_preserved() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<Integer, Integer, String> table = env.table("test", INTEGER_CODEC, INTEGER_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put(4, 3, "C");
                table.put(4, 5, "D");
                table.put(4, 2, "B");
                table.put(4, -2, "A");
                table.put(-3, 8, "C");
                table.put(-3, 6, "B");
                table.put(-3, 9, "D");
                table.put(-3, -45, "A");

                assertThat(collect(table.rowEntries(4)), contains(
                        new TableEntry<>(4, -2, "A"),
                        new TableEntry<>(4, 2, "B"),
                        new TableEntry<>(4, 3, "C"),
                        new TableEntry<>(4, 5, "D")
                ));

                assertThat(collect(table.rowEntries(-3)), contains(
                        new TableEntry<>(-3, -45, "A"),
                        new TableEntry<>(-3, 6, "B"),
                        new TableEntry<>(-3, 8, "C"),
                        new TableEntry<>(-3, 9, "D")
                ));

                assertThat(collect(table.entries()), contains(
                        new TableEntry<>(-3, -45, "A"),
                        new TableEntry<>(-3, 6, "B"),
                        new TableEntry<>(-3, 8, "C"),
                        new TableEntry<>(-3, 9, "D"),
                        new TableEntry<>(4, -2, "A"),
                        new TableEntry<>(4, 2, "B"),
                        new TableEntry<>(4, 3, "C"),
                        new TableEntry<>(4, 5, "D")
                ));
            }
        }
    }

    @Test public void
    can_reverse_the_order_of_keys() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<Integer, Integer, String> table = env.table("test", INTEGER_CODEC, INTEGER_CODEC.reverseOrder(), STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put(4, 3, "C");
                table.put(4, 5, "D");
                table.put(4, 2, "B");
                table.put(4, -2, "A");
                table.put(-3, 8, "C");
                table.put(-3, 6, "B");
                table.put(-3, 9, "D");
                table.put(-3, -45, "A");

                assertThat(collect(table.entries()), contains(
                        new TableEntry<>(-3, 9, "D"),
                        new TableEntry<>(-3, 8, "C"),
                        new TableEntry<>(-3, 6, "B"),
                        new TableEntry<>(-3, -45, "A"),
                        new TableEntry<>(4, 5, "D"),
                        new TableEntry<>(4, 3, "C"),
                        new TableEntry<>(4, 2, "B"),
                        new TableEntry<>(4, -2, "A")
                ));
            }
        }
    }

    @Test public void
    can_use_comparator_of_deserialized_type() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test",
                    STRING_CODEC.comparedUsing(String::compareToIgnoreCase),
                    STRING_CODEC.comparedUsing(String::compareTo),
                    STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("a", "1", "");
                table.put("b", "2", "");
                table.put("c", "3", "");
                table.put("A", "4", "");
                table.put("B", "5", "");
                table.put("C", "x", "");
                table.put("C", "y", "");
                table.put("C", "Z", "");

                assertThat(collect(table.entries()), contains(
                        new TableEntry<>("a", "1", ""),
                        new TableEntry<>("A", "4", ""),
                        new TableEntry<>("b", "2", ""),
                        new TableEntry<>("B", "5", ""),
                        new TableEntry<>("c", "3", ""),
                        new TableEntry<>("C", "Z", ""),
                        new TableEntry<>("C", "x", ""),
                        new TableEntry<>("C", "y", "")
                ));
            }
        }
    }

    @Test public void
    when_row_and_col_keys_combine_to_the_same_value_correct_entries_are_returned_when_using_custom_comparators() {
        try (LmdbStorageEnvironment env = createEnv()) {
            LmdbTable<String, String, String> table = env.table("test",
                    STRING_CODEC.comparedUsing(String::compareTo),
                    STRING_CODEC.comparedUsing(String::compareTo),
                    STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                table.put("A", "Aa", "1");
                table.put("A", "Ab", "2");
                table.put("A", "Ac", "3");
                table.put("AA", "a", "4");
                table.put("AA", "b", "5");
                table.put("AA", "c", "6");

                assertThat(collect(table.rowEntries("A")), contains(
                        new TableEntry<>("A", "Aa", "1"),
                        new TableEntry<>("A", "Ab", "2"),
                        new TableEntry<>("A", "Ac", "3")
                ));
                assertThat(collect(table.rowEntries("AA")), contains(
                        new TableEntry<>("AA", "a", "4"),
                        new TableEntry<>("AA", "b", "5"),
                        new TableEntry<>("AA", "c", "6")
                ));
            }
        }
    }

    //todo: test what happens with empty colKey and comparator now
}