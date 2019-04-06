package jheister.lmdbcollections.codec;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface Codec<T> {
    Codec<String> STRING_CODEC = new StringCodec();

    Codec<Integer> INTEGER_CODEC = new IntegerCodec();

    Codec<Empty> EMPTY_CODEC = new Codec<>() {
        @Override
        public Empty deserialize(ByteBuffer buffer) {
            return Empty.INSTANCE;
        }

        @Override
        public void serialize(Empty value, ByteBuffer target) {
        }
    };

    interface Empty {
        Empty INSTANCE = new Empty() {};
    }

    T deserialize(ByteBuffer buffer);

    void serialize(T value, ByteBuffer target);

    default Comparator<ByteBuffer> comparator() {
        return null;
    }

    default Codec<T> reverseOrder() {
        return new Codec<T>() {
            @Override
            public T deserialize(ByteBuffer buffer) {
                return Codec.this.deserialize(buffer);
            }

            @Override
            public void serialize(T value, ByteBuffer target) {
                Codec.this.serialize(value, target);
            }

            @Override
            public Comparator<ByteBuffer> comparator() {
                return Codec.this.comparator().reversed();
            }
        };
    }
}
