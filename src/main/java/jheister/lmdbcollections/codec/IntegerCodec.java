package jheister.lmdbcollections.codec;

import java.nio.ByteBuffer;

public class IntegerCodec implements Codec<Integer> {
    @Override
    public Integer deserialize(ByteBuffer buffer) {
        return buffer.getInt();
    }

    @Override
    public void serialize(Integer value, ByteBuffer target) {
        target.putInt(value);
    }
}
