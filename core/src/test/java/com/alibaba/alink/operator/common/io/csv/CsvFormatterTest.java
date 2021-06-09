package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class CsvFormatterTest extends AlinkTestBase {
	@Test
	public void testFormatter() throws Exception {
		TypeInformation[] types = new TypeInformation[] {Types.STRING, Types.DOUBLE, Types.LONG,
			Types.BOOLEAN, Types.SQL_TIMESTAMP};

		Row row = Row.of("string", 1.0, 1L, true, new java.sql.Timestamp(System.currentTimeMillis()));
		CsvFormatter formatter = new CsvFormatter(types, ",", '"');
		CsvParser parser = new CsvParser(types, ",", '"');
		String text = formatter.format(row);
		Tuple2 <Boolean, Row> parsed = parser.parse(text);
		Assert.assertTrue(parsed.f0);
		Assert.assertEquals(parsed.f1.getArity(), row.getArity());
		for (int i = 0; i < parsed.f1.getArity(); i++) {
			Assert.assertEquals(parsed.f1.getField(i), row.getField(i));
		}
	}

	@Test
	public void testDoublePrecision() throws Exception {
		TypeInformation[] types = new TypeInformation[] {Types.DOUBLE};

		CsvFormatter formatter = new CsvFormatter(types, ",", '"');
		CsvParser parser = new CsvParser(types, ",", '"');

		Double[] values = new Double[] {Double.MAX_VALUE, Double.MIN_VALUE, Double.NEGATIVE_INFINITY,
			Double.POSITIVE_INFINITY,
			new Random().nextDouble()};
		for (Double v : values) {
			String text = formatter.format(Row.of(v));
			Tuple2 <Boolean, Row> parsed = parser.parse(text);
			Double p = (Double) parsed.f1.getField(0);
			Assert.assertEquals(v, p, 0.);
		}
	}
}