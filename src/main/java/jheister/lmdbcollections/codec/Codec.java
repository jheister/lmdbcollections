package jheister.lmdbcollections.codec;

import java.nio.ByteBuffer;

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

}
