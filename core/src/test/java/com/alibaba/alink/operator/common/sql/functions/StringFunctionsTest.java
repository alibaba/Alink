package com.alibaba.alink.operator.common.sql.functions;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

public class StringFunctionsTest extends AlinkTestBase {

	@Test
	public void testFromBase64() throws InvocationTargetException, IllegalAccessException {
		String v = "SGVsbG9Xb3JsZA==";
		String expected = "HelloWorld";
		Assert.assertEquals(expected, StringFunctions.FROMBASE64.invoke(null, v));
	}

	@Test
	public void testFromBase64Special() throws InvocationTargetException, IllegalAccessException {
		String v = "SGVsbG9Xb3JsZA==";
		String expected = "HelloWorld";
		Assert.assertEquals(expected, StringFunctions.fromBase64(v.getBytes(StandardCharsets.UTF_8)));
	}

	@Test
	public void testToBase64() throws InvocationTargetException, IllegalAccessException {
		String v = "HelloWorld";
		String expected = "SGVsbG9Xb3JsZA==";
		Assert.assertEquals(expected, StringFunctions.TOBASE64.invoke(null, v));
	}

	@Test
	public void testLpad() throws InvocationTargetException, IllegalAccessException {
		String base = "hi";
		int len = 4;
		String pad = "??";
		String expected = "??hi";
		Assert.assertEquals(expected, StringFunctions.LPAD.invoke(null, base, len, pad));
	}

	@Test
	public void testLpadSpecial() throws InvocationTargetException, IllegalAccessException {
		String base = "hi";
		String pad = "??";
		Assert.assertNull(StringFunctions.LPAD.invoke(null, base, -1, pad));
		Assert.assertEquals("", StringFunctions.LPAD.invoke(null, base, 0, pad));
	}

	@Test
	public void testRpad() throws InvocationTargetException, IllegalAccessException {
		String base = "hi";
		int len = 4;
		String pad = "??";
		String expected = "hi??";
		Assert.assertEquals(expected, StringFunctions.RPAD.invoke(null, base, len, pad));
	}

	@Test
	public void testRpadSpecial() throws InvocationTargetException, IllegalAccessException {
		String base = "hi";
		String pad = "??";
		Assert.assertNull(StringFunctions.RPAD.invoke(null, base, -1, pad));
		Assert.assertEquals("", StringFunctions.RPAD.invoke(null, base, 0, pad));
	}

	@Test
	public void testRegexpReplace() throws InvocationTargetException, IllegalAccessException {
		String str = "foobar";
		String regex = "oo|ar";
		String replacement = "";
		String expected = "fb";
		Assert.assertEquals(expected, StringFunctions.REGEXP_REPLACE.invoke(null, str, regex, replacement));
	}

	@Test
	public void testRegexpReplaceSpecial() throws InvocationTargetException, IllegalAccessException {
		String str = "foobar";
		String regex = "oo|ar";
		String replacement = "";
		Assert.assertNull(StringFunctions.REGEXP_REPLACE.invoke(null, null, regex, replacement));
		Assert.assertNull(StringFunctions.REGEXP_REPLACE.invoke(null, str, null, replacement));
	}

	@Test
	public void testRegexpReplaceException() {
		String str = "foobar";
		String regex = "{oo|ar";
		String replacement = "";
		Assert.assertNull(StringFunctions.regexpReplace(str, regex, replacement));
	}

	@Test
	public void testRegexpExtract() throws InvocationTargetException, IllegalAccessException {
		String str = "foothebar";
		String regex = "foo(.*?)(bar)";
		int extractIndex = 2;
		String expected = "bar";
		Assert.assertEquals(expected, StringFunctions.REGEXP_EXTRACT.invoke(null, str, regex, extractIndex));
		Assert.assertEquals(null, StringFunctions.REGEXP_EXTRACT.invoke(null, str, "fao", extractIndex));
	}

	@Test
	public void testRegexpExtractSpecial() throws InvocationTargetException, IllegalAccessException {
		String str = "foothebar";
		String regex = "foo(.*?)(bar)";
		int extractIndex = 2;
		Assert.assertNull(StringFunctions.REGEXP_EXTRACT.invoke(null, null, regex, extractIndex));
		Assert.assertNull(StringFunctions.REGEXP_EXTRACT.invoke(null, str, null, extractIndex));
	}

	@Test
	public void testRegexpExtractException() {
		String str = "foobar";
		String regex = "{oo|ar";
		int extractIndex = 2;
		Assert.assertNull(StringFunctions.regexpExtract(str, regex, extractIndex));
	}

	@Test
	public void testHash() throws InvocationTargetException, IllegalAccessException {
		String str = "HelloWorld";
		String expectedMd5 = StringFunctions.md5(str);
		String expectedSha1 = StringFunctions.sha1(str);
		String expectedSha224 = StringFunctions.sha224(str);
		String expectedSha256 = StringFunctions.sha256(str);
		String expectedSha384 = StringFunctions.sha384(str);
		String expectedSha512 = StringFunctions.sha512(str);
		String expectedSha2 = StringFunctions.sha2(str, 384);

		Assert.assertEquals(expectedMd5, StringFunctions.MD5.invoke(null, str));
		Assert.assertEquals(expectedSha1, StringFunctions.SHA1.invoke(null, str));
		Assert.assertEquals(expectedSha224, StringFunctions.SHA224.invoke(null, str));
		Assert.assertEquals(expectedSha256, StringFunctions.SHA256.invoke(null, str));
		Assert.assertEquals(expectedSha384, StringFunctions.SHA384.invoke(null, str));
		Assert.assertEquals(expectedSha512, StringFunctions.SHA512.invoke(null, str));
		Assert.assertEquals(expectedSha2, StringFunctions.SHA2.invoke(null, str, 384));
	}

	@Test(expected = RuntimeException.class)
	public void testHashException() throws InvocationTargetException, IllegalAccessException {
		String str = "HelloWorld";
		Assert.assertNull(StringFunctions.sha2(str, 385));
	}

	@Test(expected = RuntimeException.class)
	public void testHashException2() throws InvocationTargetException, IllegalAccessException {
		String str = "HelloWorld";
		Assert.assertNull(StringFunctions.hash(str, "hash-385"));
	}
}
