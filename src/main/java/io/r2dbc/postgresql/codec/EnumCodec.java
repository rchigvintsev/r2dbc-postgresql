package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;

/**
 * Codec for {@link Enum}s. It always represents enumerations as strings using {@link Enum#name()} method.
 *
 * @author Roman Chigvintsev
 */
final class EnumCodec extends AbstractCodec<Enum<?>> {

    private final StringCodec delegate;

    /**
     * Creates new instance of this class with the given {@link ByteBufAllocator}.
     *
     * @param byteBufAllocator byte buffer allocator, must not be {@literal null}
     */
    EnumCodec(ByteBufAllocator byteBufAllocator) {
        super(getCodecType());
        this.delegate = new StringCodec(byteBufAllocator);
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "Value must not be null");
        return value.getClass().isEnum();
    }

    /**
     * Encodes the given enumeration into string using {@link Enum#name()} method.
     *
     * @param value enumeration to encode
     * @return encoded enumeration
     */
    @Override
    public Parameter doEncode(Enum<?> value) {
        Assert.requireNonNull(value, "Value must not be null");
        return delegate.doEncode(value.name());
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "Format must not be null");
        Assert.requireNonNull(type, "Type must not be null");
        return (PostgresqlObjectId.isValid(dataType) && (type == Object.class || type.isEnum()) &&
            doCanDecode(PostgresqlObjectId.valueOf(dataType), format));
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        return delegate.doCanDecode(type, format);
    }

    /**
     * Decodes the given {@link ByteBuf} into enumeration using {@link Enum#valueOf(Class, String)} method.
     *
     * @param buffer   data buffer
     * @param dataType well-known PostgreSQL type OID
     * @param format   data type format
     * @param type     desired enumeration type, must not be {@literal null}
     * @return decoded enumeration
     * @throws IllegalArgumentException if the given desired enumeration type is null or decoded enumeration value is invalid
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Enum<?> doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends Enum<?>> type) {
        Assert.requireNonNull(type, "Type must not be null");
        return Enum.valueOf((Class<? extends Enum>) type, delegate.doDecode(buffer, dataType, format, String.class));
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "Type must not be null");
        return type.isEnum();
    }

    @Override
    public Parameter encodeNull() {
        return delegate.encodeNull();
    }

    @SuppressWarnings("unchecked")
    private static Class<Enum<?>> getCodecType() {
        return (Class<Enum<?>>) ((Class<?>) Enum.class);
    }
}
