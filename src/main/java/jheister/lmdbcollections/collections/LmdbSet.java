package jheister.lmdbcollections.collections;

import jheister.lmdbcollections.codec.Codec.Empty;

import java.util.function.Consumer;

public class LmdbSet<T> {
    private final LmdbMap<T, Empty> underlying;

    public LmdbSet(LmdbMap<T, Empty> underlying) {
        this.underlying = underlying;
    }

    public void add(T value) {
        underlying.put(value, Empty.INSTANCE);
    }

    public void forEach(Consumer<? super T> consumer) {
        underlying.entries().forEach(e -> {
            consumer.accept(e.key);
        });
    }

    public void clear() {
        underlying.clear();
    }

    public boolean contains(T value) {
        return underlying.containsKey(value);
    }
}
