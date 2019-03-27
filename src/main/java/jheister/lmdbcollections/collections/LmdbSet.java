package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Codec;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

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

    public void add(Txn<ByteBuffer> txn, T value) {
        buffer.clear();
        valueCodec.serialize(value, buffer);
        buffer.flip();

        db.reserve(txn, buffer, 0);
    }

    public void forEach(Txn<ByteBuffer> txn, Consumer<? super T> consumer) {
        db.iterate(txn).forEachRemaining(e -> {
            consumer.accept(valueCodec.deserialize(e.key()));
        });
    }

    public void clear(Txn<ByteBuffer> txn) {
        db.drop(txn);
    }

    public boolean contains(Txn<ByteBuffer> txn, T value) {
        buffer.clear();
        valueCodec.serialize(value, buffer);
        buffer.flip();
        return db.get(txn, buffer) != null;
    }
}
