package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.codec.Codec;
import org.lmdbjava.Dbi;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class LmdbSet<T> {
    private final Dbi<ByteBuffer> db;
    private final Codec<T> valueCodec;
    private final ThreadLocal<Transaction> threadLocalTransaction;

    public LmdbSet(Dbi<ByteBuffer> db, Codec<T> valueCodec, ThreadLocal<Transaction> threadLocalTransaction) {
        this.db = db;
        this.valueCodec = valueCodec;
        this.threadLocalTransaction = threadLocalTransaction;
    }

    public void add(T value) {
        Transaction txn = localTransaction();
        txn.serializeKey(valueCodec, value);
        db.reserve(txn.lmdbTxn, txn.keyBuffer, 0);
    }

    public void forEach(Consumer<? super T> consumer) {
        Transaction txn = localTransaction();
        db.iterate(txn.lmdbTxn).forEachRemaining(e -> {
            consumer.accept(valueCodec.deserialize(e.key()));
        });
    }

    public void clear() {
        Transaction txn = localTransaction();
        db.drop(txn.lmdbTxn);
    }

    public boolean contains(T value) {
        Transaction txn = localTransaction();
        txn.keyBuffer.clear();
        valueCodec.serialize(value, txn.keyBuffer);
        txn.keyBuffer.flip();
        return db.get(txn.lmdbTxn, txn.keyBuffer) != null;
    }

    private Transaction localTransaction() {
        Transaction txn = threadLocalTransaction.get();
        if (txn == null) {
            throw new RuntimeException("Not in a transaction");
        }
        return txn;
    }
}
