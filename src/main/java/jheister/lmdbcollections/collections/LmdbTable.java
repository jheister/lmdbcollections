package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Codec;
import jheister.lmdbcollections.Transaction;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class LmdbTable<R, C, V> {
    //todo: work out concurrency issues. - concurrent read/write will fail badly
    private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(1024);//todo: work out how to specify buffer sizes etc.
    private final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(1024);

    private final Dbi<ByteBuffer> db;
    private final Codec<R> rowKeyCodec;
    private final Codec<C> colKeyCodec;
    private final Codec<V> codec;

    public LmdbTable(Dbi<ByteBuffer> db, Codec<R> rowKeyCodec, Codec<C> colKeyCodec, Codec<V> codec) {
        this.db = db;
        this.rowKeyCodec = rowKeyCodec;
        this.colKeyCodec = colKeyCodec;
        this.codec = codec;
    }

    public void put(Transaction txn, R rowKey, C colKey, V value) {
        fillKeyBuffer(rowKey, colKey);
        valueBuffer.clear();
        codec.serialize(value, valueBuffer);
        valueBuffer.flip();
        db.put(txn.lmdbTxn, keyBuffer, valueBuffer);
    }

    public V get(Transaction txn, R rowKey, C colKey) {
        fillKeyBuffer(rowKey, colKey);

        ByteBuffer valueBuffer = db.get(txn.lmdbTxn, keyBuffer);
        if (valueBuffer == null) {
            return null;
        }
        return codec.deserialize(valueBuffer);
    }

    public void remove(Transaction txn, R rowKey, C colKey) {
        fillKeyBuffer(rowKey, colKey);

        db.delete(txn.lmdbTxn, keyBuffer);
    }

    //todo: replace with forEach to avoid open streams?
    public Stream<Entry<R, C, V>> rowEntries(Transaction txn, R rowKey) {
        keyBuffer.clear();
        fillRowKey(rowKey);
        keyBuffer.flip();

        CursorIterator<ByteBuffer> iterator = db.iterate(txn.lmdbTxn, KeyRange.atLeast(keyBuffer));

        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(e -> {
                    ByteBuffer key = e.key();
                    int rowKeyLength = key.getInt();

                    //todo: optimize, no need to redo deserialize of row key every entry
                    R actualRowKey = rowKeyCodec.deserialize(key.slice().limit(rowKeyLength));

                    key.position(key.position() + rowKeyLength);
                    C colKey = colKeyCodec.deserialize(key);

                    return new Entry<>(
                            actualRowKey,
                            colKey,
                            codec.deserialize(e.val())
                    );
                }).takeWhile(e -> rowKey.equals(e.rowKey)).onClose(iterator::close);
    }

    private void fillKeyBuffer(R rowKey, C colKey) {
        keyBuffer.clear();

        fillRowKey(rowKey);
        colKeyCodec.serialize(colKey, keyBuffer);
        keyBuffer.flip();
    }

    private void fillRowKey(R rowKey) {
        keyBuffer.position(4);
        rowKeyCodec.serialize(rowKey, keyBuffer);
        int rowKeySize = keyBuffer.position() - 4;
        keyBuffer.putInt(0, rowKeySize);
    }

    public static class Entry<R, C, V> {
        public final R rowKey;
        public final C colKey;
        public final V value;


        public Entry(R rowKey, C colKey, V value) {
            this.rowKey = rowKey;
            this.colKey = colKey;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "rowKey=" + rowKey +
                    ", colKey=" + colKey +
                    ", value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry<?, ?, ?> entry = (Entry<?, ?, ?>) o;
            return Objects.equals(rowKey, entry.rowKey) &&
                    Objects.equals(colKey, entry.colKey) &&
                    Objects.equals(value, entry.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowKey, colKey, value);
        }
    }
}
