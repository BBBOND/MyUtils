package Encrypt;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by 伟阳 on 2016/1/26.
 */
public class Md5AndShaEncrypt {

    /**
     * 向外界提供的加密算法
     * @param str
     * @return
     */
    public static String encrypt(String str) {
        return md5AndSha(str);
    }

    /**
     * 双重加密，先进行md5加密，在进行sha加密
     * @param str
     * @return
     */
    private static String md5AndSha(String str) {
        return sha(md5(str));
    }

    /**
     * md5加密
     * @param str
     * @return
     */
    private static String md5(String str) {
        return encrypt(str, "md5");
    }

    /**
     * sha加密
     * @param str
     * @return
     */
    private static String sha(String str) {
        return encrypt(str, "sha-1");
    }

    /**
     * 加密通用算法
     * @param str
     * @param algorithmName
     * @return
     */
    private static String encrypt(String str, String algorithmName) {
        if (str == null || "".equals(str.trim())) {
            throw new IllegalArgumentException("无加密内容");
        }
        if (algorithmName == null || "".equals(algorithmName.trim())) {
            algorithmName = "md5";
        }
        String encryptText = null;
        try {
            MessageDigest m = MessageDigest.getInstance(algorithmName);
            m.update(str.getBytes("UTF-8"));
            byte[] s = m.digest();
            encryptText = hex(s);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encryptText;
    }

    /**
     * 转化为16进制
     * @param bytes
     * @return
     */
    private static String hex(byte[] bytes) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(Integer.toHexString((bytes[i] & 0xFF) | 0x100).substring(1, 3));
        }
        return sb.toString();
    }

}
