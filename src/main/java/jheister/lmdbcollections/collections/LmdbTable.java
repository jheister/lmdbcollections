package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.codec.Codec;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class LmdbTable<R, C, V> {
    private final Dbi<ByteBuffer> db;
    private final Codec<R> rowKeyCodec;
    private final Codec<C> colKeyCodec;
    private final Codec<V> codec;
    private final ThreadLocal<Transaction> threadLocalTransaction;

    public LmdbTable(Dbi<ByteBuffer> db, Codec<R> rowKeyCodec, Codec<C> colKeyCodec, Codec<V> codec, ThreadLocal<Transaction> threadLocalTransaction) {
        this.db = db;
        this.rowKeyCodec = rowKeyCodec;
        this.colKeyCodec = colKeyCodec;
        this.codec = codec;
        this.threadLocalTransaction = threadLocalTransaction;
    }

    public void put(R rowKey, C colKey, V value) {
        Transaction txn = localTransaction();
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);

        txn.serializeValue(codec, value);
        db.put(txn.lmdbTxn, txn.keyBuffer, txn.valueBuffer);
    }

    public V get(R rowKey, C colKey) {
        Transaction txn = localTransaction();
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);

        ByteBuffer valueBuffer = db.get(txn.lmdbTxn, txn.keyBuffer);
        if (valueBuffer == null) {
            return null;
        }
        return codec.deserialize(valueBuffer);
    }

    public boolean containsRow(R rowKey) {
        //todo: optimize
        return rowEntries(rowKey).iterator().hasNext();
    }

    public void remove(R rowKey, C colKey) {
        Transaction txn = localTransaction();
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);

        db.delete(txn.lmdbTxn, txn.keyBuffer);
    }

    public Stream<Entry<C, V>> rowEntries(R rowKey) {
        Transaction txn = localTransaction();
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
                            colKey,
                            codec.deserialize(e.val())
                    );
                }).onClose(iterator::close);
    }

    //todo: express this as a composite codec
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

    private Transaction localTransaction() {
        Transaction txn = threadLocalTransaction.get();
        if (txn == null) {
            throw new RuntimeException("Not in a transaction");
        }
        return txn;
    }
}
