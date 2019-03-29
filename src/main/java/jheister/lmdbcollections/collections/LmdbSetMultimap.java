package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.codec.Codec.Empty;

import java.util.stream.Stream;

public class LmdbSetMultimap<K, V> {
    private final LmdbTable<K, V, Empty> table;

    public LmdbSetMultimap(LmdbTable<K, V, Empty> table) {
        this.table = table;
    }

    public void put(K key, V value) {
        table.put(key, value, Empty.INSTANCE);
    }

    public void remove(K key, V value) {
        table.remove(key, value);
    }

    //todo: whilst cursors get collected when txn is finished, is it an issue in long running txn?
    public Stream<V> get(K key) {
        return table.rowEntries(key).map(e -> e.key);
    }
}
