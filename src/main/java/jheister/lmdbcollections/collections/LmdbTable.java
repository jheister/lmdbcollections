package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Codec;
import jheister.lmdbcollections.Transaction;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class LmdbTable<R, C, V> {
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
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);
        txn.valueBuffer.clear();
        codec.serialize(value, txn.valueBuffer);
        txn.valueBuffer.flip();
        db.put(txn.lmdbTxn, txn.keyBuffer, txn.valueBuffer);
    }

    public V get(Transaction txn, R rowKey, C colKey) {
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);

        ByteBuffer valueBuffer = db.get(txn.lmdbTxn, txn.keyBuffer);
        if (valueBuffer == null) {
            return null;
        }
        return codec.deserialize(valueBuffer);
    }

    public void remove(Transaction txn, R rowKey, C colKey) {
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);

        db.delete(txn.lmdbTxn, txn.keyBuffer);
    }

    public Stream<Entry<R, C, V>> rowEntries(Transaction txn, R rowKey) {
        txn.keyBuffer.clear();
        fillRowKey(txn.keyBuffer, rowKey);
        txn.keyBuffer.flip();

        int expectedKeyLength = txn.keyBuffer.getInt();
        byte[] expectedKeyBytes = new byte[expectedKeyLength];
        txn.keyBuffer.get(expectedKeyBytes);
        txn.keyBuffer.rewind();

        CursorIterator<ByteBuffer> iterator = db.iterate(txn.lmdbTxn, KeyRange.atLeast(txn.keyBuffer));


        byte[] actualKeyBytes = new byte[expectedKeyLength];

        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .takeWhile(e -> {
                    ByteBuffer key = e.key();
                    int keyLength = key.getInt();
                    if (expectedKeyLength != keyLength) {
                        return false;
                    }

                    key.get(actualKeyBytes);
                    return Arrays.equals(actualKeyBytes, expectedKeyBytes);
                })
                .map(e -> {
                    C colKey = colKeyCodec.deserialize(e.key());

                    return new Entry<>(
                            rowKey,
                            colKey,
                            codec.deserialize(e.val())
                    );
                }).onClose(iterator::close);
    }

    private void fillKeyBuffer(ByteBuffer keyBuffer, R rowKey, C colKey) {
        keyBuffer.clear();

        fillRowKey(keyBuffer, rowKey);
        colKeyCodec.serialize(colKey, keyBuffer);
        keyBuffer.flip();
    }

    private void fillRowKey(ByteBuffer keyBuffer, R rowKey) {
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
