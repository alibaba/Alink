package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.AggLookup;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggLookupTest extends AlinkTestBase {

	Row[] array1 = new Row[] {
		Row.of("1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4")
	};
	MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array1),
		new String[] {"seq0", "seq1", "seq2", "seq3", "seq4"});

	Row[] array3 = new Row[] {
		Row.of("1,2", "1,2,3", "1,2,3,4", "1,2,4", "4")
	};
	MemSourceBatchOp data1 = new MemSourceBatchOp(Arrays.asList(array3),
		new String[] {"seq0", "seq1", "seq2", "seq3", "seq4"});

	Row[] array2 = new Row[] {
		Row.of("1", "1.0,2.0,3.0,4.0"),
		Row.of("2", "2.0,3.0,4.0,5.0"),
		Row.of("3", "3.0,2.0,3.0,4.0"),
		Row.of("4", "4.0,5.0,6.0,5.0")
	};
	MemSourceBatchOp embedding = new MemSourceBatchOp(Arrays.asList(array2), new String[] {"id", "vec"});

	@Test
	public void testAggLookup() {

		AggLookupBatchOp lookup = new AggLookupBatchOp()
			.setClause("CONCAT(seq0,3) as e0, AVG(seq1) as e1, SUM(seq2) as e2,MAX(seq3) as e3,MIN(seq4) as e4")
			.setDelimiter(",")
			.setReservedCols();
		Row row = lookup.linkFrom(embedding, data).collect().get(0);

		assert (row.getField(0).toString().equals("1.0 2.0 3.0 4.0 2.0 3.0 4.0 5.0 3.0 2.0 3.0 4.0"));
		assert (row.getField(1).toString().equals("2.5 3.0 4.0 4.5"));
		assert (row.getField(2).toString().equals("10.0 12.0 16.0 18.0"));
		assert (row.getField(3).toString().equals("4.0 5.0 6.0 5.0"));
		assert (row.getField(4).toString().equals("1.0 2.0 3.0 4.0"));
	}

	@Test
	public void testAggLookupPipe() throws Exception {

		AggLookup lookup = new AggLookup().setModelData(embedding)
			.setClause("CONCAT(seq0,3) as e0, AVG(seq1) as e1, SUM(seq2) as e2,MAX(seq3) as e3,MIN(seq4) as e4")
			.setDelimiter(",")
			.setReservedCols();
		Row row = lookup.transform(data).collect().get(0);
		assert (row.getField(0).toString().equals("1.0 2.0 3.0 4.0 2.0 3.0 4.0 5.0 3.0 2.0 3.0 4.0"));
		assert (row.getField(1).toString().equals("2.5 3.0 4.0 4.5"));
		assert (row.getField(2).toString().equals("10.0 12.0 16.0 18.0"));
		assert (row.getField(3).toString().equals("4.0 5.0 6.0 5.0"));
		assert (row.getField(4).toString().equals("1.0 2.0 3.0 4.0"));
	}

	@Test
	public void testAggLookup1() throws Exception {

		new AggLookupBatchOp()
			.setClause(
				"CONCAT(seq0, 5) as e0, CONCAT(seq1) as e1, CONCAT(seq2) as e2,CONCAT(seq3) as e3,CONCAT(seq4) as e4")
			.setDelimiter(",")
			.setReservedCols(new String[] {}).linkFrom(embedding, data1).lazyPrint(1);

		new AggLookupBatchOp()
			.setClause("AVG(seq0) as e0, AVG(seq1) as e1, AVG(seq2) as e2,AVG(seq3) as e3,AVG(seq4) as e4")
			.setDelimiter(",")
			.setReservedCols(new String[] {}).linkFrom(embedding, data1).lazyPrint(1);

		new AggLookupBatchOp()
			.setClause("SUM(seq0) as e0, SUM(seq1) as e1, SUM(seq2) as e2,SUM(seq3) as e3,SUM(seq4) as e4")
			.setDelimiter(",")
			.setReservedCols(new String[] {}).linkFrom(embedding, data1).lazyPrint(1);

		new AggLookupBatchOp()
			.setClause("MAX(seq0) as e0, MAX(seq1) as e1, MAX(seq2) as e2,MAX(seq3) as e3,MAX(seq4) as e4")
			.setDelimiter(",")
			.setReservedCols(new String[] {}).linkFrom(embedding, data1).lazyPrint(1);

		new AggLookupBatchOp()
			.setClause("MIN(seq0) as e0, MIN(seq1) as e1, MIN(seq2) as e2,MIN(seq3) as e3,MIN(seq4) as e4")
			.setDelimiter(",")
			.setReservedCols(new String[] {}).linkFrom(embedding, data1).lazyPrint(1);
		BatchOperator.execute();
	}
}
