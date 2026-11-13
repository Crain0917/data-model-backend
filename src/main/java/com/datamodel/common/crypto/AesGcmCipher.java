package com.datamodel.common.crypto;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.config.AppProperties;
import org.springframework.stereotype.Component;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM。密文格式：v1:&lt;iv_b64&gt;:&lt;ciphertext+tag_b64&gt;。
 * 密钥只从环境变量 AES_KEY 注入，未配置时启动失败。
 */
@Component
public class AesGcmCipher {

    private static final String PREFIX = "v1";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 12;
    private static final int TAG_BITS = 128;
    private static final int KEY_BYTES = 32;

    private final SecretKeySpec key;
    private final SecureRandom random = new SecureRandom();

    public AesGcmCipher(AppProperties appProperties) {
        this.key = new SecretKeySpec(resolveKey(appProperties.getSecurity().getAesKey()), "AES");
    }

    public String encrypt(String plainText) {
        try {
            byte[] iv = new byte[IV_LENGTH];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            Base64.Encoder encoder = Base64.getEncoder();
            return PREFIX + ":" + encoder.encodeToString(iv) + ":" + encoder.encodeToString(encrypted);
        } catch (Exception e) {
            throw new BizException(ErrorCode.INTERNAL_ERROR, "密码加密失败");
        }
    }

    public String decrypt(String cipherText) {
        if (cipherText == null || cipherText.trim().isEmpty()) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "数据源密码为空或无法解密");
        }
        try {
            String[] parts = cipherText.split(":");
            if (parts.length != 3 || !PREFIX.equals(parts[0])) {
                throw new BizException(ErrorCode.DRIVER_ERROR, "数据源密码格式不受支持，请重新保存密码");
            }
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] iv = decoder.decode(parts[1]);
            byte[] encrypted = decoder.decode(parts[2]);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
        } catch (AEADBadTagException e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "数据源密码解密失败，请检查 AES_KEY 是否与服务一致");
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "数据源密码解密失败");
        }
    }

    private static byte[] resolveKey(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalStateException("AES_KEY 未配置，请通过环境变量注入 32 字节密钥（base64 或 32 字符原文）");
        }
        String value = raw.trim();
        try {
            byte[] decoded = Base64.getDecoder().decode(value);
            if (decoded.length == KEY_BYTES) {
                return decoded;
            }
        } catch (IllegalArgumentException ignored) {
            // 非 base64，按原文处理
        }
        byte[] rawBytes = value.getBytes(StandardCharsets.UTF_8);
        if (rawBytes.length == KEY_BYTES) {
            return rawBytes;
        }
        throw new IllegalStateException("AES_KEY 长度不合法，需 32 字节（base64 编码或 32 字符原文）");
    }
}
