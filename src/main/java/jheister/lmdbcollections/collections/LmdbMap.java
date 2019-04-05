package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment.ThreadLocalTransaction;
import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.codec.Codec;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;

import java.nio.ByteBuffer;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class LmdbMap<K, V> {
    private final Dbi<ByteBuffer> db;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;
    private final ThreadLocalTransaction localTxn;

    public LmdbMap(Dbi<ByteBuffer> db, Codec<K> keyCodec, Codec<V> valueCodec, ThreadLocalTransaction localTxn) {
        this.db = db;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.localTxn = localTxn;
    }

    public void put(K key, V value) {
        Transaction txn = localTxn.get();;

        txn.serializeKey(keyCodec, key);
        txn.serializeValue(valueCodec, value);

        db.put(txn.lmdbTxn, txn.keyBuffer, txn.valueBuffer);
    }

    public V get(K key) {
        Transaction txn = localTxn.get();;
        txn.serializeKey(keyCodec, key);
        ByteBuffer valueBuffer = db.get(txn.lmdbTxn, txn.keyBuffer);
        if (valueBuffer == null) {
            return null;
        }
        return valueCodec.deserialize(valueBuffer);
    }

    public boolean containsKey(K key) {
        Transaction txn = localTxn.get();;
        txn.serializeKey(keyCodec, key);
        return db.get(txn.lmdbTxn, txn.keyBuffer) != null;
    }

    public void remove(K key) {
        Transaction txn = localTxn.get();;
        txn.serializeKey(keyCodec, key);
        db.delete(txn.lmdbTxn, txn.keyBuffer);
    }

    public Stream<Entry<K, V>> entries() {
        Transaction txn = localTxn.get();;
        CursorIterator<ByteBuffer> iterator = db.iterate(txn.lmdbTxn);
        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false).map(e -> {
            return new Entry<K, V>(keyCodec.deserialize(e.key()), valueCodec.deserialize(e.val()));
        }).onClose(iterator::close);
    }
}
