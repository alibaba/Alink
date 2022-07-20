package com.alibaba.alink.operator.common.sql.functions;

import org.apache.flink.table.utils.EncodingUtils;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import org.apache.calcite.linq4j.tree.Types;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringFunctions implements Serializable {

	private static final long serialVersionUID = -5180924757333229653L;
	public static Method FROMBASE64 = Types.lookupMethod(StringFunctions.class, "fromBase64", String.class);
	public static Method TOBASE64 = Types.lookupMethod(StringFunctions.class, "toBase64", String.class);

	public static Method LPAD = Types.lookupMethod(StringFunctions.class, "lpad", String.class, int.class,
		String.class);
	public static Method RPAD = Types.lookupMethod(StringFunctions.class, "rpad", String.class, int.class,
		String.class);
	public static Method REGEXP_REPLACE = Types.lookupMethod(StringFunctions.class, "regexpReplace", String.class,
		String.class, String.class);
	public static Method REGEXP_EXTRACT = Types.lookupMethod(StringFunctions.class, "regexpExtract", String.class,
		String.class, int.class);

	public static Method MD5 = Types.lookupMethod(StringFunctions.class, "md5", String.class);
	public static Method SHA1 = Types.lookupMethod(StringFunctions.class, "sha1", String.class);
	public static Method SHA224 = Types.lookupMethod(StringFunctions.class, "sha224", String.class);
	public static Method SHA256 = Types.lookupMethod(StringFunctions.class, "sha256", String.class);
	public static Method SHA384 = Types.lookupMethod(StringFunctions.class, "sha384", String.class);
	public static Method SHA512 = Types.lookupMethod(StringFunctions.class, "sha512", String.class);
	public static Method SHA2 = Types.lookupMethod(StringFunctions.class, "sha2", String.class, Integer.class);

	public static String toBase64(String str) {
		return toBase64(str.getBytes(StandardCharsets.UTF_8));
	}

	public static String toBase64(byte[] bytes) {
		return Base64.getEncoder().encodeToString(bytes);
	}

	public static String fromBase64(String bs) {
		return new String(Base64.getDecoder().decode(bs.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
	}

	public static String fromBase64(byte[] bytes) {
		return new String(Base64.getDecoder().decode(bytes), StandardCharsets.UTF_8);
	}

	public static String lpad(String base, int len, String pad) {
		if (len < 0 || "".equals(pad)) {
			return null;
		}

		char[] data = new char[len];
		char[] baseChars = base.toCharArray();
		char[] padChars = pad.toCharArray();

		int pos = Math.max(len - base.length(), 0);
		for (int i = 0; i < pos; i += pad.length()) {
			for (int j = 0; j < pad.length(); j += 1) {
				if (i + j < pos) {
					data[i + j] = padChars[j];
				} else {
					break;
				}
			}
		}
		for (int i = 0; i < base.length(); i += 1) {
			if (pos + i < len) {
				data[pos + i] = baseChars[i];
			} else {
				break;
			}
		}
		return new String(data);
	}

	public static String rpad(String base, int len, String pad) {
		if (len < 0 || "".equals(pad)) {
			return null;
		} else if (len == 0) {
			return "";
		}

		char[] data = new char[len];
		char[] baseChars = base.toCharArray();
		char[] padChars = pad.toCharArray();

		int pos = 0;
		for (pos = 0; pos < base.length(); pos += 1) {
			if (pos < len) {
				data[pos] = baseChars[pos];
			} else {
				break;
			}
		}
		while (pos < len) {
			for (int i = 0; i < pad.length(); i += 1) {
				if (pos + i < len) {
					data[pos + i] = padChars[i];
				} else {
					break;
				}
			}
			pos += pad.length();
		}
		return new String(data);
	}

	public static String regexpReplace(String str, String regex, String replacement) {
		if (str == null || regex == null || replacement == null) {
			return null;
		}
		try {
			return str.replaceAll(regex, Matcher.quoteReplacement(replacement));
		} catch (Exception e) {
			return null;
		}
	}

	public static String regexpExtract(String str, String regex, int extractIndex) {
		if (str == null || regex == null) {
			return null;
		}

		try {
			Matcher m = Pattern.compile(regex).matcher(str);
			if (m.find()) {
				MatchResult mr = m.toMatchResult();
				return mr.group(extractIndex);
			}
		} catch (Exception e) {
		}
		return null;
	}

	public static String hash(String str, MessageDigest md) {
		return EncodingUtils.hex(md.digest(str.getBytes(StandardCharsets.UTF_8)));
	}

	public static String hash(String str, String algorithm) {
		if (null == str) {
			return null;
		}
		try {
			MessageDigest instance = MessageDigest.getInstance(algorithm);
			return hash(str, instance);
		} catch (NoSuchAlgorithmException e) {
			throw new AkUnsupportedOperationException(String.format("Algorithm for %s is not available.", algorithm), e);
		}
	}

	public static String md5(String str) {
		return hash(str, "MD5");
	}

	public static String sha1(String str) {
		return hash(str, "SHA");
	}

	public static String sha224(String str) {
		return hash(str, "SHA-224");
	}

	public static String sha256(String str) {
		return hash(str, "SHA-256");
	}

	public static String sha384(String str) {
		return hash(str, "SHA-384");
	}

	public static String sha512(String str) {
		return hash(str, "SHA-512");
	}

	public static String sha2(String str, Integer bitLen) {
		if (null == bitLen) {
			return null;
		}
		if ((bitLen == 224) || (bitLen == 256) || (bitLen == 384) || (bitLen == 512)) {
			return hash(str, "SHA-" + bitLen);
		} else {
			throw new AkUnsupportedOperationException("Unsupported algorithm for bitLen " + bitLen);
		}
	}
}
