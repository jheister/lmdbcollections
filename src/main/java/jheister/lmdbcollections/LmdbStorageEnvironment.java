package jheister.lmdbcollections;

import jheister.lmdbcollections.codec.Codec;
import jheister.lmdbcollections.collections.LmdbMap;
import jheister.lmdbcollections.collections.LmdbSet;
import jheister.lmdbcollections.collections.LmdbSetMultimap;
import jheister.lmdbcollections.collections.LmdbTable;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

import java.io.File;
import java.nio.ByteBuffer;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LmdbStorageEnvironment implements AutoCloseable {
    public static final int LMDB_MAX_KEY = 511;
    public static final int MAX_VALUE_SIZE = 1024;

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

    public static LmdbStorageEnvironment create(File path, int maxCollections, long maxTotalSize) {
        return new LmdbStorageEnvironment(Env.create()
                .setMapSize(maxTotalSize)
                .setMaxDbs(maxCollections)
                .open(path, EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOTLS));
    }

    //todo: work out how to cache transaction objects / their buffers - count should match max transactions
    public Transaction txnWrite() {
        return new Transaction(env.txnWrite(), ByteBuffer.allocateDirect(LMDB_MAX_KEY), ByteBuffer.allocateDirect(MAX_VALUE_SIZE), this::checkin);
    }

    public Transaction txnRead() {
        return new Transaction(env.txnRead(), ByteBuffer.allocateDirect(LMDB_MAX_KEY), ByteBuffer.allocateDirect(MAX_VALUE_SIZE), this::checkin);
    }

    private void checkin(Transaction transaction) {

    }
}
