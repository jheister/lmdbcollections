package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment.ThreadLocalTransaction;
import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.codec.Codec;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;

import java.nio.ByteBuffer;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class LmdbSetMultimap<K, V> {

    private final Dbi<ByteBuffer> db;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;
    private final ThreadLocalTransaction localTxn;

    public LmdbSetMultimap(Dbi<ByteBuffer> db, Codec<K> keyCodec, Codec<V> valueCodec, ThreadLocalTransaction localTxn) {
        this.db = db;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.localTxn = localTxn;
    }

    public void put(K key, V value) {
        Transaction txn = localTxn.get();
        txn.serializeKey(keyCodec, key);
        txn.serializeValue(valueCodec, value);
        db.put(txn.lmdbTxn, txn.keyBuffer, txn.valueBuffer);
    }

    public void remove(K key, V value) {
        Transaction txn = localTxn.get();
        txn.serializeKey(keyCodec, key);
        txn.serializeValue(valueCodec, value);

        db.delete(txn.lmdbTxn, txn.keyBuffer, txn.valueBuffer);
    }

    public Stream<V> get(K key) {
        Transaction txn = localTxn.get();
        txn.serializeKey(keyCodec, key);
        CursorIterator<ByteBuffer> iterator = db.iterate(txn.lmdbTxn, KeyRange.atLeast(txn.keyBuffer));

        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .takeWhile(e -> e.key().compareTo(txn.keyBuffer) == 0)
                .map(e -> valueCodec.deserialize(e.val())).onClose(iterator::close);
    }
}
