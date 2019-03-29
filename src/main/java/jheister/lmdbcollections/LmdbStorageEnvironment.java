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
import java.util.Objects;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
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

    public static class Stats {
        public final String name;
        public final long branchPages;
        public final int depth;
        public final long entries;
        public final long leafPages;
        public final long overflowPages;
        public final int pageSize;

        public Stats(String name,
                     long branchPages,
                     int depth,
                     long entries,
                     long leafPages,
                     long overflowPages,
                     int pageSize) {
            this.name = name;
            this.branchPages = branchPages;
            this.depth = depth;
            this.entries = entries;
            this.leafPages = leafPages;
            this.overflowPages = overflowPages;
            this.pageSize = pageSize;
        }

        public long size() {
            return leafSize() + branchSize() + overflowSize();
        }

        public long overflowSize() {
            return overflowPages * pageSize;
        }

        public long leafSize() {
            return leafPages * pageSize;
        }

        public long branchSize() {
            return branchPages * pageSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return branchPages == stats.branchPages &&
                    depth == stats.depth &&
                    entries == stats.entries &&
                    leafPages == stats.leafPages &&
                    overflowPages == stats.overflowPages &&
                    pageSize == stats.pageSize &&
                    Objects.equals(name, stats.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, branchPages, depth, entries, leafPages, overflowPages, pageSize);
        }

        @Override
        public String toString() {
            return "Stats{" +
                    "name='" + name + '\'' +
                    ", branchPages=" + branchPages +
                    ", depth=" + depth +
                    ", entries=" + entries +
                    ", leafPages=" + leafPages +
                    ", overflowPages=" + overflowPages +
                    ", pageSize=" + pageSize +
                    '}';
        }
    }
}
