package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.Codec;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class LmdbMap<K, V> {
    //todo: work out concurrency issues. - concurrent read/write will fail badly
    private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(1024);
    private final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(1024);

    private final Dbi<ByteBuffer> db;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    public LmdbMap(Dbi<ByteBuffer> db, Codec<K> keyCodec, Codec<V> valueCodec) {
        this.db = db;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public void put(Txn<ByteBuffer> txn, K key, V value) {
        fillKeyBuffer(key);
        valueBuffer.clear();
        valueCodec.serialize(value, valueBuffer);
        valueBuffer.flip();
        db.put(txn, keyBuffer, valueBuffer);
    }

    public V get(Txn<ByteBuffer> txn, K key) {
        fillKeyBuffer(key);
        ByteBuffer valueBuffer = db.get(txn, keyBuffer);
        if (valueBuffer == null) {
            return null;
        }
        return valueCodec.deserialize(valueBuffer);
    }

    public void remove(Txn<ByteBuffer> txn, K key) {
        fillKeyBuffer(key);
        db.delete(txn, keyBuffer);
    }

    //todo: replace with forEach to avoid open streams?
    public Stream<Entry<K, V>> entries(Txn<ByteBuffer> txn) {
        CursorIterator<ByteBuffer> iterator = db.iterate(txn);
        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false).map(e -> {
            return new Entry<K, V>(keyCodec.deserialize(e.key()), valueCodec.deserialize(e.val()));
        }).onClose(iterator::close);
    }

    private void fillKeyBuffer(K key) {
        keyBuffer.clear();
        keyCodec.serialize(key, keyBuffer);
        keyBuffer.flip();
    }

    public static class Entry<K, V> {
        public final K key;
        public final V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "key='" + key + '\'' +
                    ", value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry<?, ?> entry = (Entry<?, ?>) o;
            return Objects.equals(key, entry.key) &&
                    Objects.equals(value, entry.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }
}
