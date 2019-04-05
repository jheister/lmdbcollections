package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment.ThreadLocalTransaction;
import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.codec.Codec;
import org.lmdbjava.Dbi;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class LmdbSet<T> {
    private final Dbi<ByteBuffer> db;
    private final Codec<T> valueCodec;
    private final ThreadLocalTransaction localTxn;

    public LmdbSet(Dbi<ByteBuffer> db, Codec<T> valueCodec, ThreadLocalTransaction localTxn) {
        this.db = db;
        this.valueCodec = valueCodec;
        this.localTxn = localTxn;
    }

    public void add(T value) {
        Transaction txn = localTxn.get();
        txn.serializeKey(valueCodec, value);
        db.reserve(txn.lmdbTxn, txn.keyBuffer, 0);
    }

    public void forEach(Consumer<? super T> consumer) {
        Transaction txn = localTxn.get();
        db.iterate(txn.lmdbTxn).forEachRemaining(e -> {
            consumer.accept(valueCodec.deserialize(e.key()));
        });
    }

    public void clear() {
        Transaction txn = localTxn.get();
        db.drop(txn.lmdbTxn);
    }

    public boolean contains(T value) {
        Transaction txn = localTxn.get();
        txn.keyBuffer.clear();
        valueCodec.serialize(value, txn.keyBuffer);
        txn.keyBuffer.flip();
        return db.get(txn.lmdbTxn, txn.keyBuffer) != null;
    }
}
