package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Codec.Empty;
import jheister.lmdbcollections.Transaction;

import java.util.stream.Stream;

public class LmdbSetMultimap<K, V> {
    private final LmdbTable<K, V, Empty> table;

    public LmdbSetMultimap(LmdbTable<K, V, Empty> table) {
        this.table = table;
    }

    public void put(Transaction txn, K key, V value) {
        table.put(txn, key, value, Empty.INSTANCE);
    }

    public void remove(Transaction txn, K key, V value) {
        table.remove(txn, key, value);
    }

    public Stream<V> get(Transaction txn, K key) {
        return table.rowEntries(txn, key).map(e -> e.colKey);
    }
}
