package jheister.lmdbcollections;

import jheister.lmdbcollections.collections.LmdbSet;
import org.junit.Test;
import org.lmdbjava.Env;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;
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

            int uuidStringLength = UUID.randomUUID().toString().getBytes(UTF_8).length;
            int requiredValues = (1024 * 1024) / uuidStringLength;

            try (Transaction txn = env.txnWrite()) {
                for (int i = 0; i < requiredValues; i++) {
                    set.add(UUID.randomUUID().toString());
                }
            }
        }
    }

    @Test public void
    until_committed_data_is_not_visible_to_readers() throws ExecutionException, InterruptedException {
        ExecutorService anotherThread = Executors.newFixedThreadPool(1);

        try (LmdbStorageEnvironment env = createEnv(1024 * 1024)) {
            LmdbSet<String> set = env.createSet("test", STRING_CODEC);

            Transaction writeTxn = env.txnWrite();
            anotherThread.submit(env::txnRead).get();

            set.add("A");

            assertThat(set.contains("A"), is(true));

            assertThat(anotherThread.submit(() -> set.contains("A")).get(), is(false));

            writeTxn.commit();

            assertThat(anotherThread.submit(() -> set.contains("A")).get(), is(false));

            env.txnRead();
            assertThat(set.contains("A"), is(true));
        }
    }

    @Test public void
    transactions_apply_across_entire_env() {
        try (LmdbStorageEnvironment env = createEnv(1024 * 1024)) {
            LmdbSet<String> set1 = env.createSet("test1", STRING_CODEC);
            LmdbSet<String> set2 = env.createSet("test2", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set1.add("A");
                set2.add("B");
            }

            try (Transaction txn = env.txnRead()) {
                assertThat(set1.contains("A"), is(false));
                assertThat(set2.contains("B"), is(false));
            }
        }
    }

    @Test public void
    can_get_stats_for_the_entire_env() {
        try (LmdbStorageEnvironment env = createEnv(1024 * 1024)) {
            LmdbSet<String> set1 = env.createSet("test1", STRING_CODEC);
            LmdbSet<String> set2 = env.createSet("test2", STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                set1.add("A");
                set2.add("B");
            }

            //todo: assert results
            env.stats().forEach((k, v) -> {
                System.out.println(k + ": " + v);
            });
        }
    }

    //todo: look at using custom comparator + how it works
}