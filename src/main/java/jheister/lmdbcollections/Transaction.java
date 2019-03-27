package jheister.lmdbcollections;

import org.lmdbjava.Txn;

import java.nio.ByteBuffer;

public class Transaction implements AutoCloseable {
    public final Txn<ByteBuffer> lmdbTxn;
    public final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(1024);
    public final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(1024);

    public Transaction(Txn<ByteBuffer> lmdbTxn) {
        this.lmdbTxn = lmdbTxn;
    }

    @Override
    public void close() {
        lmdbTxn.close();
    }

    public void commit() {
        lmdbTxn.commit();
    }
}
