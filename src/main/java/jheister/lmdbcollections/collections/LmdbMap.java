package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.codec.Codec;
import jheister.lmdbcollections.Transaction;
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

    public LmdbMap(Dbi<ByteBuffer> db, Codec<K> keyCodec, Codec<V> valueCodec) {
        this.db = db;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public void put(Transaction txn, K key, V value) {
        fillKeyBuffer(txn.keyBuffer, key);
        txn.valueBuffer.clear();
        valueCodec.serialize(value, txn.valueBuffer);
        txn.valueBuffer.flip();
        db.put(txn.lmdbTxn, txn.keyBuffer, txn.valueBuffer);
    }

    public V get(Transaction txn, K key) {
        fillKeyBuffer(txn.keyBuffer, key);
        ByteBuffer valueBuffer = db.get(txn.lmdbTxn, txn.keyBuffer);
        if (valueBuffer == null) {
            return null;
        }
        return valueCodec.deserialize(valueBuffer);
    }

    public void remove(Transaction txn, K key) {
        fillKeyBuffer(txn.keyBuffer, key);
        db.delete(txn.lmdbTxn, txn.keyBuffer);
    }

    public Stream<Entry<K, V>> entries(Transaction txn) {
        CursorIterator<ByteBuffer> iterator = db.iterate(txn.lmdbTxn);
        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false).map(e -> {
            return new Entry<K, V>(keyCodec.deserialize(e.key()), valueCodec.deserialize(e.val()));
        }).onClose(iterator::close);
    }

    private void fillKeyBuffer(ByteBuffer keyBuffer, K key) {
        keyBuffer.clear();
        keyCodec.serialize(key, keyBuffer);
        keyBuffer.flip();
    }

}
