package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.client.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.*;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.*;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.*;
import static io.r2dbc.postgresql.util.ByteBufUtils.*;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.*;

/**
 * @author Roman Chigvintsev
 */
class EnumCodecTest {

    private static final int dataType = VARCHAR.getObjectId();

    private EnumCodec codec;

    @BeforeEach
    void setUp() {
        this.codec = new EnumCodec(TEST);
    }

    @Test
    void canEncode() {
        assertThat(codec.canEncode(TestEnum.HELLO)).isTrue();
        assertThat(codec.canEncode("HELLO")).isFalse();
        assertThat(codec.canEncode(0)).isFalse();
    }

    @Test
    void canEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.canEncode(null))
            .withMessage("Value must not be null");
    }

    @Test
    void doEncode() {
        assertThat(codec.doEncode(TestEnum.HELLO)).hasFormat(FORMAT_TEXT).hasType(dataType)
            .hasValue(encode(TEST, "HELLO"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doEncode(null))
            .withMessage("Value must not be null");
    }

    @Test
    void canDecode() {
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, TestEnum.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, Object.class)).isTrue();

        assertThat(codec.canDecode(-1, FORMAT_TEXT, TestEnum.class)).isFalse();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, String.class)).isFalse();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, Integer.class)).isFalse();
    }

    @Test
    void canDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.canDecode(dataType, null, TestEnum.class))
            .withMessage("Format must not be null");
    }

    @Test
    void canDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.canDecode(dataType, FORMAT_TEXT, null))
            .withMessage("Type must not be null");
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(BPCHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(CHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(TEXT, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(UNKNOWN, FORMAT_TEXT)).isTrue();

        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doDecode() {
        assertThat(codec.doDecode(encode(TEST, "HELLO"), VARCHAR, FORMAT_TEXT, TestEnum.class))
            .isEqualTo(TestEnum.HELLO);
    }

    @Test
    void doDecodeInvalidValue() {
        assertThatIllegalArgumentException().isThrownBy(() ->
            codec.doDecode(encode(TEST, "BYE"), VARCHAR, FORMAT_TEXT, TestEnum.class))
            .withMessageStartingWith("No enum constant");
    }

    @Test
    void canEncodeNull() {
        assertThat(codec.canEncodeNull(TestEnum.class)).isTrue();

        assertThat(codec.canEncodeNull(String.class)).isFalse();
        assertThat(codec.canEncodeNull(Integer.class)).isFalse();
    }

    @Test
    void canEncodeNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.canEncodeNull(null))
            .withMessage("Type must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(codec.encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, VARCHAR.getObjectId(), NULL_VALUE));
    }

    private enum TestEnum {
        HELLO
    }
}
