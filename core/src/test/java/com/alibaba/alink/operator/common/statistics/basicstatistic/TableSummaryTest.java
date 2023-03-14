package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for TableSummary.
 */

public class TableSummaryTest extends AlinkTestBase {

	@Test
	public void test() {
		TableSummary srt = testVisit();
		System.out.println(srt.toString());

		Assert.assertEquals(srt.colNames.length, 5);
		Assert.assertEquals(srt.count(), 4);
		Assert.assertEquals(srt.numMissingValue("f_double"), 1, 10e-4);
		Assert.assertEquals(srt.numValidValue("f_double"), 3, 10e-4);
		Assert.assertEquals(srt.maxDouble("f_double"), 2.0, 10e-4);
		Assert.assertEquals(srt.minDouble("f_int"), 0.0, 10e-4);
		Assert.assertEquals(srt.mean("f_double"), 0.3333333333333333, 10e-4);
		Assert.assertEquals(srt.variance("f_double"), 8.333333333333334, 10e-4);
		Assert.assertEquals(srt.standardDeviation("f_double"), 2.886751345948129, 10e-4);
		//Assert.assertEquals(srt.normL1("f_double"), 7.0, 10e-4);
		Assert.assertEquals(srt.normL2("f_double"), 4.123105625617661, 10e-4);

	}

	private TableSummary testVisit() {
		Row[] data =
			new Row[] {
				Row.of("a", 1L, 1, 2.0, true),
				Row.of(null, 2L, 2, -3.0, true),
				Row.of("c", null, null, 2.0, false),
				Row.of("a", 0L, 0, null, null),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		TypeInformation <?>[] colTypes = new TypeInformation <?>[]{
			Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN
		};

		TableSchema schema = new TableSchema(colNames, colTypes);
		TableSummarizer summarizer = new TableSummarizer(schema, false);
		for (Row aData : data) {
			summarizer.visit(aData);
		}

		return summarizer.toSummary();
	}
}