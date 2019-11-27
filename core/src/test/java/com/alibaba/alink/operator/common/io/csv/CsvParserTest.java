package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CsvParserTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testParser() throws Exception {
        CsvParser parser = new CsvParser(new TypeInformation[]{Types.STRING}, ",", '"');
        Assert.assertEquals(parser.parse("\"hello, world\"").getField(0), "hello, world");
        Assert.assertEquals(parser.parse("").getField(0), null);
        Assert.assertEquals(parser.parse("\"\"").getField(0), "");
        Assert.assertEquals(parser.parse("\"\"\"\"\"\"").getField(0), "\"\"");
    }

    @Test
    public void testLongFieldSeparator() throws Exception {
        CsvParser parser = new CsvParser(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, "____", '"');
        Assert.assertEquals(parser.parse("hello_____world____").getField(0), "hello");
        Assert.assertEquals(parser.parse("hello_____world____").getField(1), "_world");
        Assert.assertEquals(parser.parse("hello_____world____").getField(2), null);
        Assert.assertEquals(parser.parse("\"hello_____world____\"").getField(0), "hello_____world____");
        Assert.assertEquals(parser.parse("\"hello_____world____\"").getField(1), null);
        Assert.assertEquals(parser.parse("\"hello_____world____\"").getField(2), null);
    }

    @Test
    public void testMalFormatString1() throws Exception {
        CsvParser parser = new CsvParser(new TypeInformation[]{Types.STRING}, ",", '"');
        thrown.expect(RuntimeException.class);
        parser.parse("\"hello\" world"); // should end with quote
    }

    @Test
    public void testMalFormatString2() throws Exception {
        CsvParser parser = new CsvParser(new TypeInformation[]{Types.STRING}, ",", '"');
        thrown.expect(RuntimeException.class);
        parser.parse("\"hello world"); // unterminated quote
    }
}