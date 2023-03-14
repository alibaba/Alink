package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.collections.map.ListOrderedMap;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
		Assert.assertEquals(srt.maxDouble("f_double"), 2.0, 10e-4);
		Assert.assertEquals(srt.minDouble("f_int"), 0.0, 10e-4);
		Assert.assertEquals(srt.mean("f_double"), 0.3333333333333333, 10e-4);
		Assert.assertEquals(srt.variance("f_double"), 8.333333333333334, 10e-4);
		Assert.assertEquals(srt.standardDeviation("f_double"), 2.886751345948129, 10e-4);
		//Assert.assertEquals(srt.normL1("f_double"), 7.0, 10e-4);
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
			.setSelectedCols("long_1", "long_2", "long_3", "long_4", "long_5");

		source.link(allStat).lazyPrintSummary();

		BatchOperator.execute();
	}

	@Test
	public void testMultiType() {
		Map <String, Object[]> data = new HashMap <>();
		data.put("f01_string", new Object[] {"a", null, "c", "a"});
		data.put("f02_long", new Object[] {1111111111111L, 22222222L, null, 0L});
		data.put("f03_int", new Object[] {1, 2, null, 0});
		data.put("f04_short", new Object[] {new Short("1"), new Short("2"), null, new Short("0")});
		data.put("f05_byte", new Object[] {new Byte("1"), new Byte("1"), null, new Byte("0")});
		data.put("f06_double", new Object[] {2.2, -3.4, 2.6, null});
		data.put("f07_float", new Object[] {1.2f, 2.1f, null, -0.2f});
		data.put("f08_boolean", new Object[] {true, true, false, null});
		data.put("f09_timestamp",
			new Object[] {new Timestamp(100000), new Timestamp(20000), null, new Timestamp(30000)});
		data.put("f10_date", new Object[] {new Date(1), null, new Date(2), new Date(3)});
		data.put("f11_time", new Object[] {new Time(1), null, new Time(1), new Time(1)});
		data.put("f12_decimal", new Object[] {
			new BigDecimal("2.345"),
			new BigDecimal("3.345"),
			new BigDecimal("4.345"),
			null}
		);
		data.put("f13_dense_vector", new Object[] {
			VectorUtil.getDenseVector("1.0 2.0"),
			null,
			VectorUtil.getDenseVector("-1.0 2.0"),
			VectorUtil.getDenseVector("1.0 -2.0")}
		);
		data.put("f14_sparse_vector", new Object[] {
			VectorUtil.getSparseVector("$3$0:1.0 2:2.0"),
			null,
			VectorUtil.getSparseVector("$3$0:1.0 2:2.0"),
			VectorUtil.getSparseVector("$3$0:1.0 2:2.0")}
		);
		data.put("f15_mtable", new Object[] {
			new MTable(new Integer[] {1, 2}, "col"),
			null,
			new MTable(new Integer[] {-1, 2}, "col"),
			new MTable(new Integer[] {1, -2}, "col")}
		);
		//print tensor must have shape.
		data.put("f16_tensor", new Object[] {
			new DoubleTensor(1.0).reshape(new Shape(1)),
			null,
			new DoubleTensor(2.0).reshape(new Shape(1)),
			new DoubleTensor(3.0).reshape(new Shape(1))}
		);
		data.put("f17_binary", new Object[] {"aaaa".getBytes(), null, "abc".getBytes(), "ac".getBytes()});
		data.put("f18_list", new Object[] {
			Collections.singletonList(1),
			null,
			Collections.singletonList(2),
			Collections.singletonList(3)}
		);
		data.put("f19_map", new Object[] {new HashMap <>(), null, new HashMap <>(), new HashMap <>()});
		data.put("f20_array", new Object[] {new int[] {1, 2}, null, new int[] {-1, 2}, new int[] {1, -2}});

		String[] colNames = data.keySet().toArray(new String[0]);
		Arrays.sort(colNames);
		int rowNum = 4;
		Row[] testArray = new Row[rowNum];
		for (int i = 0; i < rowNum; i++) {
			testArray[i] = new Row(colNames.length);
			for (int j = 0; j < colNames.length; j++) {
				testArray[i].setField(j, data.get(colNames[j])[i]);
			}
		}

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		source.lazyPrint(2);

		TableSummary summary = source.collectStatistics();
		System.out.println(summary);

		Assert.assertEquals(20, summary.getColNames().length);
	}

}