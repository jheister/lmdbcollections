package jheister.lmdbcollections;

import jheister.lmdbcollections.collections.LmdbSet;
import org.junit.Test;
import org.lmdbjava.Env;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static jheister.lmdbcollections.codec.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LmdbStorageEnvironmentTest extends TestBase {
    @Test
    public void
    fails_when_storage_size_is_exceeded() {
        thrown.expect(Env.MapFullException.class);

        try (LmdbStorageEnvironment env = createEnv(1024 * 1024)) {
            LmdbSet<String> set = env.createSet("test", STRING_CODEC);

            int uuidStringLength = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8).length;
            int requiredValues = (1024 * 1024) / uuidStringLength;

            try (Transaction txn = env.txnWrite()) {
                for (int i = 0; i < requiredValues; i++) {
                    set.add(txn, UUID.randomUUID().toString());
                }
            }
        }
    }

    @Test public void
    until_committed_data_is_not_visible_to_readers() {
        try (LmdbStorageEnvironment env = createEnv(1024 * 1024)) {
            LmdbSet<String> set = env.createSet("test", STRING_CODEC);

            Transaction writeTxn = env.txnWrite();
            Transaction readTxn = env.txnRead();

            set.add(writeTxn, "A");

            assertThat(set.contains(writeTxn, "A"), is(true));
            assertThat(set.contains(readTxn, "A"), is(false));

            writeTxn.commit();

            assertThat(set.contains(readTxn, "A"), is(false));
            readTxn.close();

            assertThat(set.contains(env.txnRead(), "A"), is(true));
        }
    }

    @Test public void
    transactions_apply_across_entire_env() {
        try (LmdbStorageEnvironment env = createEnv(1024 * 1024)) {
            LmdbSet<String> set1 = env.createSet("test1", STRING_CODEC);
            LmdbSet<String> set2 = env.createSet("test2", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set1.add(txn, "A");
                set2.add(txn, "B");
            }

            try (Transaction txn = env.txnRead()) {
                assertThat(set1.contains(txn, "A"), is(false));
                assertThat(set2.contains(txn, "B"), is(false));
            }
        }
    }

    @Test public void
    can_have_multiple_readers_in_one_thread() {
        try (LmdbStorageEnvironment env = createEnv(1024 * 1024)) {
            LmdbSet<String> set1 = env.createSet("test1", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set1.add(txn, "A");
                txn.commit();
            }

            try (Transaction txn1 = env.txnRead();
                 Transaction txn2 = env.txnRead()) {
                assertThat(set1.contains(txn1, "A"), is(true));
                assertThat(set1.contains(txn2, "A"), is(true));
            }
        }
    }
}