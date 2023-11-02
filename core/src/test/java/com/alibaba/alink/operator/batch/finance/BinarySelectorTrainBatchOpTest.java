package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class BinarySelectorTrainBatchOpTest extends AlinkTestBase {

	private BatchOperator data;

	@Test
	public void testNormal() throws Exception {
		geneData();
		BinarySelectorTrainBatchOp selector =
			new BinarySelectorTrainBatchOp()
				.setAlphaEntry(0.05)
				.setAlphaStay(0.05)
				.setSelectedCol("vec")
				.setLabelCol("label")
				.setForceSelectedCols(new int[] {1});

		selector.linkFrom(data).print();

	}

	@Test
	public void testStringLabel() throws Exception {
		geneData();
		BinarySelectorTrainBatchOp selector =
			new BinarySelectorTrainBatchOp()
				.setAlphaEntry(0.05)
				.setAlphaStay(0.05)
				.setSelectedCol("vec")
				.setLabelCol("label_string")
				.setForceSelectedCols(new int[] {1});

		selector.linkFrom(data).print();
	}

	private void geneData() {
		Row[] rows = new Row[] {
			Row.of("$3$0:1.0 1:7.0 2:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, "3", 3),
			Row.of("$3$0:1.0 1:3.0 2:3.0", "2.0 3.0 3.0", 1.0, 3.0, 3.0, "3", 3),
			Row.of("$3$0:1.0 1:2.0 2:4.0", "3.0 2.0 4.0", 1.0, 2.0, 4.0, "2", 2),
			Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 3.0 4.0", 1.0, 3.0, 4.0, "2", 2),
			Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 5.0 8.0", 1.0, 3.0, 4.0, "2", 2),
			Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 6.0 3.0", 1.0, 3.0, 4.0, "2", 2)
		};
		String[] colNames = new String[] {"sparse_vec", "vec", "f0", "f1", "f2", "label_string", "label"};
		this.data = new MemSourceBatchOp(Arrays.asList(rows), colNames);
	}

}