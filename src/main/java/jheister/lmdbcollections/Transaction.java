package jheister.lmdbcollections;

import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class Transaction implements AutoCloseable {
    public final Txn<ByteBuffer> lmdbTxn;
    public final ByteBuffer keyBuffer;
    public final ByteBuffer valueBuffer;
    private final Consumer<Transaction> checkinFunction;

    public Transaction(Txn<ByteBuffer> lmdbTxn,
                       ByteBuffer keyBuffer,
                       ByteBuffer valueBuffer,
                       Consumer<Transaction> checkinFunction) {
        this.lmdbTxn = lmdbTxn;
        this.keyBuffer = keyBuffer;
        this.valueBuffer = valueBuffer;
        this.checkinFunction = checkinFunction;
    }

    @Override
    public void close() {
        checkinFunction.accept(this);
        lmdbTxn.close();
    }

    public void commit() {
        lmdbTxn.commit();
    }
}
