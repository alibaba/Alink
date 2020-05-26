package com.alibaba.alink.common.sql.functions;

import com.alibaba.alink.operator.common.sql.functions.StringFunctions;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

public class StringFunctionsTest {

    @Test
    public void testFromBase64() throws InvocationTargetException, IllegalAccessException {
        String v = "SGVsbG9Xb3JsZA==";
        String expected = "HelloWorld";
        Assert.assertEquals(expected, StringFunctions.FROMBASE64.invoke(null, v));
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
    public void testRpad() throws InvocationTargetException, IllegalAccessException {
        String base = "hi";
        int len = 4;
        String pad = "??";
        String expected = "hi??";
        Assert.assertEquals(expected, StringFunctions.RPAD.invoke(null, base, len, pad));
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
    public void testRegexpExtract() throws InvocationTargetException, IllegalAccessException {
        String str = "foothebar";
        String regex = "foo(.*?)(bar)";
        int extractIndex = 2;
        String expected = "bar";
        Assert.assertEquals(expected, StringFunctions.REGEXP_EXTRACT.invoke(null, str, regex, extractIndex));
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

}
