package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.codec.Codec;
import jheister.lmdbcollections.Transaction;
import org.lmdbjava.Dbi;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class LmdbSet<T> {
    private final Dbi<ByteBuffer> db;
    private final Codec<T> valueCodec;

    public LmdbSet(Dbi<ByteBuffer> db, Codec<T> valueCodec) {
        this.db = db;
        this.valueCodec = valueCodec;
    }

    public void add(Transaction txn, T value) {
        txn.keyBuffer.clear();
        valueCodec.serialize(value, txn.keyBuffer);
        txn.keyBuffer.flip();

        db.reserve(txn.lmdbTxn, txn.keyBuffer, 0);
    }

    public void forEach(Transaction txn, Consumer<? super T> consumer) {
        db.iterate(txn.lmdbTxn).forEachRemaining(e -> {
            consumer.accept(valueCodec.deserialize(e.key()));
        });
    }

    public void clear(Transaction txn) {
        db.drop(txn.lmdbTxn);
    }

    public boolean contains(Transaction txn, T value) {
        txn.keyBuffer.clear();
        valueCodec.serialize(value, txn.keyBuffer);
        txn.keyBuffer.flip();
        return db.get(txn.lmdbTxn, txn.keyBuffer) != null;
    }
}
