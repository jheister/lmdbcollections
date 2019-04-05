package jheister.lmdbcollections.codec;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static java.util.Comparator.comparingInt;

public class IntegerCodec implements Codec<Integer> {
    @Override
    public Integer deserialize(ByteBuffer buffer) {
        return buffer.getInt();
    }

    @Override
    public void serialize(Integer value, ByteBuffer target) {
        target.putInt(value);
    }

    @Override
    public Comparator<ByteBuffer> comparator() {
        return comparingInt(ByteBuffer::getInt);
    }
}
