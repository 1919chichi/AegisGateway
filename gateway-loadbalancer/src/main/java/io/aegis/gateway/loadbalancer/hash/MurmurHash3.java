package io.aegis.gateway.loadbalancer.hash;

/**
 * MurmurHash3（32-bit，x86 变体）手写实现。
 * <p>
 * 只在 {@link ConsistentHashRing} 构建虚拟节点位置、计算 key 在环上的落点时使用，是包内
 * 可见的无状态纯函数工具类，不对外暴露为公共 API 契约。之所以手写而非引入依赖：编译期
 * classpath 上没有 Guava；netty-common（含 {@code io.netty.util.internal.MurmurHash3}）
 * 虽然作为 Nacos 客户端的传递依赖存在，但位于 {@code internal} 包、没有兼容性保证，不适合
 * 直接依赖（详见设计文档"背景与约束"D9）。
 * <p>
 * 算法与业界标准 MurmurHash3_x86_32 完全一致，已用参考实现交叉验证过多组固定输入的期望值
 * （见 {@link MurmurHash3Test}），覆盖 4 字节对齐边界的全部尾部分支。
 */
final class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private MurmurHash3() {}

    static int hash(byte[] data, int seed) {
        int h1 = seed;
        int length = data.length;
        int roundedEnd = length & 0xfffffffc; // 向下取整到 4 字节边界

        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff)
                    | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16)
                    | (data[i + 3] << 24);
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // 处理末尾不足 4 字节的部分（rem = 1/2/3），rem = 0 时该 switch 不匹配任何分支
        int k1 = 0;
        switch (length & 0x03) {
            case 3:
                k1 ^= (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 ^= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 ^= (data[roundedEnd] & 0xff);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
                break;
        }

        h1 ^= length;
        h1 = fmix(h1);
        return h1;
    }

    private static int fmix(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}
