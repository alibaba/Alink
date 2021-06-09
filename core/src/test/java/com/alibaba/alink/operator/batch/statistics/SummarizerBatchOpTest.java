package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class SummarizerBatchOpTest extends AlinkTestBase {

	@Test
	public void test() {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1L, 1, 2.0, true),
				Row.of(null, 2L, 2, -3.0, true),
				Row.of("c", null, null, 2.0, false),
				Row.of("a", 0L, 0, null, null),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		SummarizerBatchOp summarizer = new SummarizerBatchOp()
			.setSelectedCols("f_double", "f_int");

		summarizer.linkFrom(source);

		TableSummary srt = summarizer.collectSummary();

		System.out.println(srt.toString());

		Assert.assertEquals(srt.getColNames().length, 2);
		Assert.assertEquals(srt.count(), 4);
		Assert.assertEquals(srt.numMissingValue("f_double"), 1, 10e-4);
		Assert.assertEquals(srt.numValidValue("f_double"), 3, 10e-4);
		Assert.assertEquals(srt.max("f_double"), 2.0, 10e-4);
		Assert.assertEquals(srt.min("f_int"), 0.0, 10e-4);
		Assert.assertEquals(srt.mean("f_double"), 0.3333333333333333, 10e-4);
		Assert.assertEquals(srt.variance("f_double"), 8.333333333333334, 10e-4);
		Assert.assertEquals(srt.standardDeviation("f_double"), 2.886751345948129, 10e-4);
		Assert.assertEquals(srt.normL1("f_double"), 7.0, 10e-4);
		Assert.assertEquals(srt.normL2("f_double"), 4.123105625617661, 10e-4);

	}

	@Test
	public void testLazy() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1L, 1, 2.0, true),
				Row.of(null, 2L, 2, -3.0, true),
				Row.of("c", null, null, 2.0, false),
				Row.of("a", 0L, 0, null, null),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		SummarizerBatchOp summarizer = new SummarizerBatchOp()
			.setSelectedCols("f_string", "f_double", "f_int");

		summarizer.linkFrom(source);

		summarizer.lazyPrintSummary();

		summarizer.lazyCollectSummary(new Consumer <TableSummary>() {
			@Override
			public void accept(TableSummary summary) {
				Assert.assertEquals(0.3333333333333333, summary.mean("f_double"), 10e-8);
			}
		});

		BatchOperator.execute();

	}

	@Test
	public void testEmptyData() throws Exception {
		TableSchema schema = new TableSchema(
			new String[] {"string_1", "long_1", "long_2", "long_3", "long_4", "long_5", "string_2", "string_3",
				"string_4"},
			new TypeInformation <?>[] {Types.STRING(), Types.LONG(), Types.LONG(),
				Types.LONG(), Types.LONG(), Types.LONG(),
				Types.STRING(), Types.STRING(), Types.STRING()}
		);

		List <Row> dataList = new ArrayList <>();

		MemSourceBatchOp source = new MemSourceBatchOp(dataList, schema);

		SummarizerBatchOp allStat = new SummarizerBatchOp()
			.setSelectedCols(new String[] {"long_1", "long_2", "long_3", "long_4", "long_5"});

		source.link(allStat).lazyPrintSummary();

		BatchOperator.execute();
	}

}