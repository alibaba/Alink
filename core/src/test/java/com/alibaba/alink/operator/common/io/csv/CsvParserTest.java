package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class CsvParserTest extends AlinkTestBase {

	@Test
	public void testParser() throws Exception {
		CsvParser parser = new CsvParser(new TypeInformation[] {Types.STRING}, ",", '"');
		Assert.assertEquals(parser.parse("\"hello, world\"").f1.getField(0), "hello, world");
		Assert.assertEquals(parser.parse("").f1.getField(0), null);
		Assert.assertEquals(parser.parse("\"\"").f1.getField(0), "");
		Assert.assertEquals(parser.parse("\"\"\"\"\"\"").f1.getField(0), "\"\"");
	}

	@Test
	public void testLongFieldSeparator() throws Exception {
		CsvParser parser = new CsvParser(new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING}, "____",
			'"');
		Assert.assertEquals(parser.parse("hello_____world____").f1.getField(0), "hello");
		Assert.assertEquals(parser.parse("hello_____world____").f1.getField(1), "_world");
		Assert.assertEquals(parser.parse("hello_____world____").f1.getField(2), null);
		Assert.assertEquals(parser.parse("\"hello_____world____\"").f1.getField(0), "hello_____world____");
		Assert.assertEquals(parser.parse("\"hello_____world____\"").f1.getField(1), null);
		Assert.assertEquals(parser.parse("\"hello_____world____\"").f1.getField(2), null);
	}

	@Test
	public void testMalFormatString1() throws Exception {
		CsvParser parser = new CsvParser(new TypeInformation[] {Types.STRING, Types.STRING}, ",", '"');
		Assert.assertFalse(parser.parse("\"hello\" world").f0);
	}

	@Test
	public void testMalFormatString2() throws Exception {
		CsvParser parser = new CsvParser(new TypeInformation[] {Types.STRING, Types.STRING}, ",", '"');
		Assert.assertFalse(parser.parse("\"hello world").f0);
	}
}