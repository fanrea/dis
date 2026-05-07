package com.dis.demo;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.nio.charset.StandardCharsets;

public class PayloadDecryptor {

    public static void main(String[] args) {
        // 原始密文
        String payload = "YA4D4i2YKnCx26ci67nehZC0nZA95BR7aHe5euC1LzjKEFw0D6mCMnuVK5pxEHmEtiPmAjpi4ViWRi7ZSN0N649HPvP3-Dya9sgZR2lkYwDBBnLk2CiMaQJRssUnyuk6S2p8Ts5UESBprM-5sTy9gr_zhzfoiJA_9mQJMtFfrSCP7a8JeF8qbY2H0oVhWgcEiHMgcQMa0gdgyFuR6k_nEpAgvP_l2iOdbrcr_jWyIJfF2ZuuYdaWUxhwJVrLe6sYl53qhq379cyCYMMvNK_eK0xT-q-iKoHcOmPme4J62YuyInkh50u_xNBFCjvHf7Adz0uUJ-tluRyHGJx3-JRtCgVKddIVZW928bJyYfTrS4eNNLwrJpIJUT0nOhKHZg0qQNVJmpLZ95fFlnqC_pKJrWVOrRU78Tfehri1g6Hid2QkuHYuS5VA6hYi8OJ7i8CBs51Ri4T37cEjQmx-cdC-llpv0qE2kwRn4QbOhoTdhvrFYeld-1-mKfp5pyqv1J-3pE63Xk4cGLdGzV4WZf9Te5Ch31y82QBUSixe3xhOS38_bNvyKDQRnf2ut_q8CCdDhQDs0u93rrOuCosbWDvBXeBoZEfB1zH7OAKkXUKeZQ0L8YLdDj-HYPNr1yDi2I8UkHq3veHUyDQOD_O2SCvd2hi40fj3NfGRCXM_av7p3yAHgor14lmiJCWw3cM5iPrXAT50yZp60mQNDIKDLQCUokiWlaHvSKxJOYm4lkve6YLskWGAJqlIxnbE9L3lQW5RRG1UkjyDnr2cWF_tirzC33asFgeSZh6v_0wxBGo5RX1lFPF0rwy4xycXJ0djwc5qUoCNGXNbm30joKijg39GZA";

        // 解析 URL Safe Base64
        byte[] encryptedBytes = Base64.getUrlDecoder().decode(payload);

        // 候选秘钥 1: AES/ECB
        tryDecrypt("AES/ECB/PKCS5Padding", "b0458c2b262949b8".getBytes(StandardCharsets.UTF_8), null, encryptedBytes);

        // 候选秘钥 2: AES/CBC
        tryDecrypt("AES/CBC/PKCS5Padding", "c78623c22e2f6513".getBytes(StandardCharsets.UTF_8), "0000000000000000".getBytes(StandardCharsets.UTF_8), encryptedBytes);

        // 候选秘钥 3: AES/ECB (来自 Base64)
        tryDecrypt("AES/ECB/PKCS5Padding", Base64.getDecoder().decode("Kxge1FYXZWov7gg01ELhcQ=="), null, encryptedBytes);

        // 候选秘钥 4: 3DES/CBC
        tryDecrypt("DESede/CBC/NoPadding", "365083dfc483d623e745e27d".getBytes(StandardCharsets.UTF_8), "00000000".getBytes(StandardCharsets.UTF_8), encryptedBytes);

        // 候选秘钥 5: AES/CBC
        tryDecrypt("AES/CBC/PKCS5Padding", "b8e13bd1e8c56cf8d14a659982bccbcf".getBytes(StandardCharsets.UTF_8), "1082040e1b4e7189".getBytes(StandardCharsets.UTF_8), encryptedBytes);

        // 候选秘钥 6: AES/CBC
        tryDecrypt("AES/CBC/PKCS5Padding", "aee90452d962602658d3b9ab70e36931".getBytes(StandardCharsets.UTF_8), "e04d276df9452572".getBytes(StandardCharsets.UTF_8), encryptedBytes);

        // 候选秘钥 7: AES/CBC
        tryDecrypt("AES/CBC/PKCS5Padding", "aec565488f9fcb6d4a6ecea0b8fbbc69".getBytes(StandardCharsets.UTF_8), "8dafcdaf8db5f645".getBytes(StandardCharsets.UTF_8), encryptedBytes);
    }

    private static void tryDecrypt(String algorithm, byte[] key, byte[] iv, byte[] cipherText) {
        try {
            Cipher cipher = Cipher.getInstance(algorithm);
            String keyAlgorithm = algorithm.split("/")[0];
            SecretKeySpec keySpec = new SecretKeySpec(key, keyAlgorithm);

            if (iv != null) {
                cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(iv));
            } else {
                cipher.init(Cipher.DECRYPT_MODE, keySpec);
            }

            byte[] decrypted = cipher.doFinal(cipherText);
            System.out.println("解密成功！");
            System.out.println("使用算法: " + algorithm);
            System.out.println("使用秘钥 (Hex): " + bytesToHex(key));
            System.out.println("解密内容: \n" + new String(decrypted, StandardCharsets.UTF_8));
            System.out.println("--------------------------------------------------");
        } catch (Exception e) {
            // 解密失败，忽略并尝试下一个
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}