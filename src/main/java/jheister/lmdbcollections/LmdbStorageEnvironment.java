package jheister.lmdbcollections;

import jheister.lmdbcollections.collections.LmdbMap;
import jheister.lmdbcollections.collections.LmdbSet;
import jheister.lmdbcollections.collections.LmdbSetMultimap;
import jheister.lmdbcollections.collections.LmdbTable;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LmdbStorageEnvironment implements AutoCloseable {
    private final Env<ByteBuffer> env;

    public LmdbStorageEnvironment(Env<ByteBuffer> env) {
        this.env = env;
    }

    public <R, C, V> LmdbTable<R, C, V> createTable(String name, Codec<R> rowKeyCodec, Codec<C> colKeyCodec, Codec<V> valueCodec) {
        return new LmdbTable<>(env.openDbi(name, MDB_CREATE), rowKeyCodec, colKeyCodec, valueCodec);
    }

    public <K, V> LmdbMap<K, V> createMap(String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        return new LmdbMap<>(env.openDbi(name, MDB_CREATE), keyCodec, valueCodec);
    }

    public <T> LmdbSet<T> createSet(String name, Codec<T> codec) {
        return new LmdbSet<T>(env.openDbi(name, MDB_CREATE), codec);
    }

    public LmdbSetMultimap<String, String> createSetMultimap(String name, Codec<String> keyCodec, Codec<String> valueCodec) {
        return new LmdbSetMultimap<>(createTable(name, keyCodec, valueCodec, Codec.EMPTY_CODEC));
    }

    @Override
    public void close() {
        env.close();
    }

    public static LmdbStorageEnvironment create(File path, long maxSize) {
        //todo: what are the min/max values? - should be pagesizes etc
        //todo: work out what a good value for maxDbs would be
        return new LmdbStorageEnvironment(Env.create()
                .setMapSize(maxSize)
                .setMaxDbs(3)
                .open(path));
    }

    public Txn<ByteBuffer> txnWrite() {
        return env.txnWrite();
    }

    public Txn<ByteBuffer> txnRead() {
        return env.txnRead();
    }
}
