package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.LmdbStorageEnvironment.ThreadLocalTransaction;
import jheister.lmdbcollections.Transaction;
import jheister.lmdbcollections.codec.Codec;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LmdbTable<R, C, V> {
    private final Dbi<ByteBuffer> db;
    private final Codec<R> rowKeyCodec;
    private final Codec<C> colKeyCodec;
    private final Codec<V> codec;
    private final ThreadLocalTransaction localTxn;

    public LmdbTable(Dbi<ByteBuffer> db, Codec<R> rowKeyCodec, Codec<C> colKeyCodec, Codec<V> codec, ThreadLocalTransaction localTxn) {
        this.db = db;
        this.rowKeyCodec = rowKeyCodec;
        this.colKeyCodec = colKeyCodec;
        this.codec = codec;
        this.localTxn = localTxn;
    }

    public void put(R rowKey, C colKey, V value) {
        Transaction txn = localTxn.get();
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);

        txn.serializeValue(codec, value);
        db.put(txn.lmdbTxn, txn.keyBuffer, txn.valueBuffer);
    }

    public V get(R rowKey, C colKey) {
        Transaction txn = localTxn.get();
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
        Transaction txn = localTxn.get();
        fillKeyBuffer(txn.keyBuffer, rowKey, colKey);

        db.delete(txn.lmdbTxn, txn.keyBuffer);
    }

    //todo: cleanup duplication splitting ByteBuffer into row / col buffers
    public Stream<TableEntry<R, C, V>> rowEntries(R rowKey) {
        Transaction txn = localTxn.get();
        txn.keyBuffer.clear();
        fillRowKey(txn.keyBuffer, rowKey);
        txn.keyBuffer.flip();

        CursorIterator<ByteBuffer> iterator = db.iterate(txn.lmdbTxn, KeyRange.atLeast(txn.keyBuffer));

        Comparator<ByteBuffer> comparator = rowKeyCodec.comparator() == null ? Comparator.naturalOrder() : rowKeyCodec.comparator();

        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .takeWhile(e -> {
                    ByteBuffer key = e.key();
                    int len = key.remaining();

                    key.limit(key.getInt() + 4);

                    txn.keyBuffer.position(4);

                    boolean stillWanted = comparator.compare(key, txn.keyBuffer) == 0;
                    key.rewind().limit(len);
                    return stillWanted;
                })
                .map(this::entryFor).onClose(iterator::close);
    }

    public Stream<TableEntry<R, C, V>> entries() {
        Transaction txn = localTxn.get();

        CursorIterator<ByteBuffer> iterator = db.iterate(txn.lmdbTxn);

        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(this::entryFor)
                .onClose(iterator::close);
    }

    private TableEntry<R, C, V> entryFor(CursorIterator.KeyVal<ByteBuffer> e) {
        int len = e.key().remaining();
        int rowKeyLen = e.key().getInt();
        e.key().limit(rowKeyLen + 4);
        R rowKey = rowKeyCodec.deserialize(e.key());
        e.key().limit(len);
        e.key().position(rowKeyLen + 4);
        C colKey = colKeyCodec.deserialize(e.key());

        return new TableEntry<>(
                rowKey,
                colKey,
                codec.deserialize(e.val())
        );
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

    public static <R, C, V> LmdbTable<R, C, V> create(Env<ByteBuffer> env,
                                                      String name,
                                                      ThreadLocalTransaction threadLocalTransaction,
                                                      Codec<R> rowCodec,
                                                      Codec<C> colCodec,
                                                      Codec<V> valCodec) {
        return new LmdbTable<>(env.openDbi(name, constructComparator(rowCodec.comparator(), colCodec.comparator()), MDB_CREATE), rowCodec, colCodec, valCodec, threadLocalTransaction);
    }

    private static Comparator<ByteBuffer> constructComparator(Comparator<ByteBuffer> providedRowComparator, Comparator<ByteBuffer> providedColComparator) {
        if (providedRowComparator == null && providedColComparator == null) {
            return null;
        } else {
            Comparator<ByteBuffer> rowComparator = providedRowComparator == null ? Comparator.naturalOrder() : providedRowComparator;
            Comparator<ByteBuffer> colComparator = providedColComparator == null ? Comparator.naturalOrder() : providedColComparator;

            return (o1, o2) -> {
                int o1Len = o1.remaining();
                int o2Len = o2.remaining();
                int o1RowKeyLen = o1.getInt();
                int o2RowKeyLen = o2.getInt();

                o1.limit(o1RowKeyLen + 4);
                o2.limit(o2RowKeyLen + 4);

                int rowKeyCompare = rowComparator.compare(o1, o2);

                o1.rewind().limit(o1Len).position(o1RowKeyLen + 4);
                o2.rewind().limit(o2Len).position(o2RowKeyLen + 4);

                if (rowKeyCompare != 0) {
                    return rowKeyCompare;
                }

                if (o1.remaining() > 0 && o2.remaining() > 0) {
                    return colComparator.compare(o1, o2);
                } else {
                    return o1Len - o2Len;
                }
            };
        }
    }

    public static class TableEntry<R, C, V> {
        public final R rowKey;
        public final C colKey;
        public final V value;

        public TableEntry(R rowKey, C colKey, V value) {
            this.rowKey = rowKey;
            this.colKey = colKey;
            this.value = value;
        }

        @Override
        public String toString() {
            return "TableEntry{" +
                    "rowKey=" + rowKey +
                    ", colKey=" + colKey +
                    ", value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableEntry<?, ?, ?> that = (TableEntry<?, ?, ?>) o;
            return Objects.equals(rowKey, that.rowKey) &&
                    Objects.equals(colKey, that.colKey) &&
                    Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowKey, colKey, value);
        }
    }
}
