package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Codec.Empty;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class LmdbSetMultimap<K, V> {
    private final LmdbTable<K, V, Empty> table;

    public LmdbSetMultimap(LmdbTable<K, V, Empty> table) {
        this.table = table;
    }

    public void put(Txn<ByteBuffer> txn, K key, V value) {
        table.put(txn, key, value, Empty.INSTANCE);
    }

    public void remove(Txn<ByteBuffer> txn, K key, V value) {
        table.remove(txn, key, value);
    }

    //todo: replace with foreach?
    public Stream<V> get(Txn<ByteBuffer> txn, K key) {
        return table.rowEntries(txn, key).map(e -> e.colKey);
    }
}
