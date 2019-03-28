package jheister.lmdbcollections;

import jheister.lmdbcollections.codec.Codec;
import jheister.lmdbcollections.collections.LmdbMap;
import jheister.lmdbcollections.collections.LmdbSet;
import jheister.lmdbcollections.collections.LmdbSetMultimap;
import jheister.lmdbcollections.collections.LmdbTable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LmdbStorageEnvironment implements AutoCloseable {
    private static final int LMDB_MAX_KEY = 511;
    private static final int MAX_VALUE_SIZE = 4096 * 10;

    private final Env<ByteBuffer> env;
    private final ThreadLocal<Transaction> threadLocalTransaction = new ThreadLocal<>();

    public LmdbStorageEnvironment(Env<ByteBuffer> env) {
        this.env = env;
    }

    public <R, C, V> LmdbTable<R, C, V> createTable(String name, Codec<R> rowKeyCodec, Codec<C> colKeyCodec, Codec<V> valueCodec) {
        return new LmdbTable<>(env.openDbi(name, MDB_CREATE), rowKeyCodec, colKeyCodec, valueCodec, threadLocalTransaction);
    }

    public <K, V> LmdbMap<K, V> createMap(String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        return new LmdbMap<>(env.openDbi(name, MDB_CREATE), keyCodec, valueCodec, threadLocalTransaction);
    }

    public <T> LmdbSet<T> createSet(String name, Codec<T> codec) {
        return new LmdbSet<T>(env.openDbi(name, MDB_CREATE), codec, threadLocalTransaction);
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
                .open(path, EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC, EnvFlags.MDB_MAPASYNC));
    }

    //todo: work out how to cache transaction objects / their buffers - count should match max transactions
    public Transaction txnWrite() {
        Transaction txn = new Transaction(env.txnWrite(), ByteBuffer.allocateDirect(LMDB_MAX_KEY), ByteBuffer.allocateDirect(MAX_VALUE_SIZE), this::checkin);
        threadLocalTransaction.set(txn);
        return txn;
    }

    public Transaction txnRead() {
        Transaction txn = new Transaction(env.txnRead(), ByteBuffer.allocateDirect(LMDB_MAX_KEY), ByteBuffer.allocateDirect(MAX_VALUE_SIZE), this::checkin);
        threadLocalTransaction.set(txn);
        return txn;
    }

    //todo: cleanup threadlocal handling
    private void checkin(Transaction transaction) {
        threadLocalTransaction.set(null);
    }

    public Map<String, Stat> stats() {
        return env.getDbiNames().stream()
                .collect(Collectors.toMap(b -> new String(b, UTF_8), b -> {
                    //todo: is it bad to open it again. Should probably be cached
                    Dbi<ByteBuffer> db = env.openDbi(b);
                    try (Txn<ByteBuffer> txn = env.txnRead()) {
                        return db.stat(txn);
                    } finally {
                        db.close();
                    }
                }));
    }
}
