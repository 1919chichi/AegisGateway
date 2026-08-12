package io.aegis.gateway.loadbalancer.hash;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class MurmurHash3Test {

    // 下面几组期望值已用 Python 参考实现（mmh3 库，业界标准 MurmurHash3_x86_32 实现）
    // 交叉验证过，覆盖 4 字节对齐边界的全部尾部分支（rem=0/1/2/3），不是随手编造的数字。

    @Test
    void hash_ofEmptyInput_shouldBeZero() {
        assertThat(MurmurHash3.hash(new byte[0], 0)).isZero();
    }

    @Test
    void hash_ofTwoByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("ab".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(-1681926305);
    }

    @Test
    void hash_ofThreeByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("abc".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(-1277324294);
    }

    @Test
    void hash_ofFourByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("test".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(-1167338989);
    }

    @Test
    void hash_ofFiveByteInput_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("hello".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(613153351);
    }

    @Test
    void hash_withDifferentSeed_shouldMatchReferenceVector() {
        assertThat(MurmurHash3.hash("test".getBytes(StandardCharsets.UTF_8), 42)).isEqualTo(-335093414);
    }

    @Test
    void hash_isDeterministic_forSameInputAndSeed() {
        byte[] data = "order-service-1-0".getBytes(StandardCharsets.UTF_8);

        int first = MurmurHash3.hash(data, 7);
        int second = MurmurHash3.hash(data, 7);

        assertThat(first).isEqualTo(second);
    }
}
