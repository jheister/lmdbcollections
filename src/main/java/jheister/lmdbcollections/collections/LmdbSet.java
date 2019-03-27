package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Codec;
import jheister.lmdbcollections.Transaction;
import org.lmdbjava.Dbi;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class LmdbSet<T> {
    //todo: work out concurrency issues. - concurrent read/write will fail badly
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

    private final Dbi<ByteBuffer> db;
    private final Codec<T> valueCodec;

    public LmdbSet(Dbi<ByteBuffer> db, Codec<T> valueCodec) {
        this.db = db;
        this.valueCodec = valueCodec;
    }

    public void add(Transaction txn, T value) {
        buffer.clear();
        valueCodec.serialize(value, buffer);
        buffer.flip();

        db.reserve(txn.lmdbTxn, buffer, 0);
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
        buffer.clear();
        valueCodec.serialize(value, buffer);
        buffer.flip();
        return db.get(txn.lmdbTxn, buffer) != null;
    }
}
