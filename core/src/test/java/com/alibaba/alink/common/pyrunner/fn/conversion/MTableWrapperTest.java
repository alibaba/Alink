package com.alibaba.alink.common.pyrunner.fn.conversion;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

public class MTableWrapperTest {

	@Test
	public void testFromJava() {
		List <Row> rows = Arrays.asList(
			Row.of(1, 1L, 1.f, 1., "abc", false),
			Row.of(2, 2L, 2.f, 2., "def", true)
		);
		String schemaStr = "i int, l long, f float, d double, s string, b boolean";
		MTable mTable = new MTable(rows, schemaStr);
		MTableWrapper wrapper = MTableWrapper.fromJava(mTable);
		String expectedContent = "1,1,1.0,1.0,abc,false\n"
			+ "2,2,2.0,2.0,def,true\n";
		Assert.assertEquals(expectedContent, wrapper.getContent());
		Assert.assertArrayEquals(new String[] {"i", "l", "f", "d", "s", "b"}, wrapper.getColNames());
		Assert.assertEquals(schemaStr, wrapper.getSchemaStr());
	}

	@Test
	public void testFromPy() {
		String csvContent = "1,1,1.0,1.0,abc,false\n"
			+ "2,2,2.0,2.0,def,true\n";
		String schemaStr = "i int, l long, f float, d double, s string, b boolean";
		MTableWrapper wrapper = MTableWrapper.fromPy(csvContent, schemaStr);
		MTable mTable = wrapper.getJavaObject();

		String content;
		try (StringWriter stringWriter = new StringWriter();
			 BufferedWriter writer = new BufferedWriter(stringWriter)) {
			mTable.writeCsvToFile(writer);
			writer.flush();
			content = stringWriter.toString();
		} catch (IOException e) {
			throw new RuntimeException("Failed to write MTable to StringWriter.", e);
		}
		String expectedSchemaStr = "i INT,l BIGINT,f FLOAT,d DOUBLE,s VARCHAR,b BOOLEAN";
		Assert.assertEquals(csvContent, content);
		Assert.assertEquals(expectedSchemaStr, mTable.getSchemaStr());
	}
}
