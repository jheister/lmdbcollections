package jheister.lmdbcollections;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StringCodec implements Codec<String> {
    @Override
    public String deserialize(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }

    @Override
    public void serialize(String value, ByteBuffer target) {
        byte[] bytes = value.getBytes(UTF_8);
        target.put(bytes);
    }
}
