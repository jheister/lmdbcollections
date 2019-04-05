package jheister.lmdbcollections;

import jheister.lmdbcollections.codec.Codec;
import jheister.lmdbcollections.collections.LmdbMap;
import jheister.lmdbcollections.collections.LmdbSet;
import jheister.lmdbcollections.collections.LmdbSetMultimap;
import jheister.lmdbcollections.collections.LmdbTable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;

public class LmdbStorageEnvironment implements AutoCloseable {
    private static final int LMDB_MAX_KEY = 511;
    private static final int MAX_VALUE_SIZE = 4096 * 10;

    private final Env<ByteBuffer> env;
    private final ThreadLocalTransaction threadLocalTransaction = new ThreadLocalTransaction();

    public LmdbStorageEnvironment(Env<ByteBuffer> env) {
        this.env = env;
    }

    public <R, C, V> LmdbTable<R, C, V> table(String name, Codec<R> rowKeyCodec, Codec<C> colKeyCodec, Codec<V> valueCodec) {
        return new LmdbTable<>(env.openDbi(name, MDB_CREATE), rowKeyCodec, colKeyCodec, valueCodec, threadLocalTransaction);
    }

    public <K, V> LmdbMap<K, V> map(String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        return new LmdbMap<>(env.openDbi(name, MDB_CREATE), keyCodec, valueCodec, threadLocalTransaction);
    }

    public <T> LmdbSet<T> set(String name, Codec<T> codec) {
        return new LmdbSet<T>(map(name, codec, Codec.EMPTY_CODEC));
    }

    public <K, V> LmdbSetMultimap<K, V> sortedSetMultimap(String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        return new LmdbSetMultimap<>(env.openDbi(name, MDB_CREATE, MDB_DUPSORT), keyCodec, valueCodec, threadLocalTransaction);
    }

    @Override
    public void close() {
        env.close();
    }

    public static LmdbStorageEnvironment create(File path, int maxCollections, long maxTotalSize) {
        return new LmdbStorageEnvironment(Env.create()
                .setMapSize(maxTotalSize)
                .setMaxDbs(maxCollections)
                .open(path));
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

    public List<Stats> stats() {
        return Stream.concat(Stream.of(stats("", env.stat())), env.getDbiNames().stream()
                .map(b -> {
                    //todo: is it bad to open it again. Should probably be cached
                    Dbi<ByteBuffer> db = env.openDbi(b);
                    try (Txn<ByteBuffer> txn = env.txnRead()) {
                        return stats(new String(b, UTF_8), db.stat(txn));
                    } finally {
                        db.close();
                    }
                })).collect(toList());
    }

    private static Stats stats(String name, Stat stat) {
        return new Stats(name, stat.branchPages, stat.depth, stat.entries, stat.leafPages, stat.overflowPages, stat.pageSize);
    }

    public static class ThreadLocalTransaction {
        private final ThreadLocal<Transaction> threadLocalTransaction = new ThreadLocal<>();

        public Transaction get() {
            Transaction txn = threadLocalTransaction.get();
            if (txn == null) {
                throw new RuntimeException("Not in a transaction");
            }
            return txn;
        }

        public void set(Transaction txn) {
            threadLocalTransaction.set(txn);
        }
    }
}
