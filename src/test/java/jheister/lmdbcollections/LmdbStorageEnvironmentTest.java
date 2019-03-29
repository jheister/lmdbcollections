package jheister.lmdbcollections;

import jheister.lmdbcollections.LmdbStorageEnvironment.Stats;
import jheister.lmdbcollections.collections.LmdbMap;
import jheister.lmdbcollections.collections.LmdbSet;
import org.junit.Ignore;
import org.junit.Test;
import org.lmdbjava.Env;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static jheister.lmdbcollections.codec.Codec.INTEGER_CODEC;
import static jheister.lmdbcollections.codec.Codec.STRING_CODEC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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
                txn.commit();
            }

            assertThat(env.stats(), contains(
                    new Stats("", 0, 1, 2, 1, 0, 4096),
                    new Stats("test1", 0, 1, 1, 1, 0, 4096),
                    new Stats("test2", 0, 1, 1, 1, 0, 4096)
            ));
        }
    }

    @Ignore
    @Test public void
    storage_efficiency_experiment() {
        try (LmdbStorageEnvironment env = createEnv((long) 1024 * 1024 * 1024 * 5)) {
            List<Integer> keys = IntStream.range(0, 10000000).boxed().collect(toList());

//            Collections.shuffle(keys);

            LmdbMap<Integer, String> map = env.createMap("test1", INTEGER_CODEC, STRING_CODEC);

            try (Transaction txn = env.txnWrite()) {
                keys.forEach(key -> {
                    map.put(key, UUID.randomUUID().toString());
                });
                txn.commit();
            }


            Stats stats = env.stats().stream().filter(s -> s.name.equals("test1")).findAny().get();
            long dataAndKeysize = keys.size() * 36 + keys.size() * 4;
            stats.size();

            System.out.println(stats);
            System.out.println("Efficiency: " + ((double) dataAndKeysize) / stats.size());
            System.out.println("Size: " + stats.size() / 1024 / 1024);
        }
    }

    //todo: look at using custom comparator + how it works
}