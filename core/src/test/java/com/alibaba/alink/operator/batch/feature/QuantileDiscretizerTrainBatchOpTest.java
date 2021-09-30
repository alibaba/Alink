package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelInfo;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;

public class QuantileDiscretizerTrainBatchOpTest extends AlinkTestBase {

	@Test
	public void linkFrom() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9),
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9)
			};

		String[] colnames = new String[] {"col1", "col2", "col3"};
		MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		QuantileDiscretizerTrainBatchOp qop = new QuantileDiscretizerTrainBatchOp()
			.setNumBuckets(3)
			.setSelectedCols(colnames[1], colnames[2])
			.linkFrom(sourceBatchOp);

		QuantileDiscretizerPredictBatchOp qpop = new QuantileDiscretizerPredictBatchOp().setEncode("VECTOR")
			.setSelectedCols(new String[] {colnames[1], colnames[2]});

		qop.lazyPrint(-1);

		qpop.linkFrom(qop, sourceBatchOp).print();
	}

	@Test
	public void distinctTest() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9),
				Row.of("a", 4, 1.1)
			};

		String[] colnames = new String[] {"col1", "col2", "col3"};
		MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		QuantileDiscretizerTrainBatchOp qop = new QuantileDiscretizerTrainBatchOp()
			.setNumBuckets(5)
			.setSelectedCols(colnames[1], colnames[2])
			.setLeftOpen(false)
			.linkFrom(sourceBatchOp);

		QuantileDiscretizerPredictBatchOp qpop = new QuantileDiscretizerPredictBatchOp()
			.setSelectedCols(colnames[1], colnames[2]);

		qop.lazyPrint(-1);
		;

		qpop.linkFrom(qop, sourceBatchOp).print();
	}

	@Test
	public void nullTest() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9),
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9),
				Row.of("e", null, 1.1)
			};

		String[] colnames = new String[] {"col1", "col2", "col3"};
		MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		QuantileDiscretizerTrainBatchOp qop = new QuantileDiscretizerTrainBatchOp()
			.setNumBuckets(3)
			.setSelectedCols(colnames[1], colnames[2])
			.linkFrom(sourceBatchOp);

		QuantileDiscretizerPredictBatchOp qpop = new QuantileDiscretizerPredictBatchOp()
			.setSelectedCols(colnames[1], colnames[2]);

		qop.lazyPrint(-1);
		;

		qpop.linkFrom(qop, sourceBatchOp).print();
	}

	@Test
	public void testTransform() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9),
				Row.of("a", 1, 1.1),
				Row.of("b", -2, 0.9),
				Row.of("c", 100, -0.01),
				Row.of("d", -99, 100.9),
				Row.of("e", null, 1.1)
			};

		String[] colnames = new String[] {"col1", "col2", "col3"};
		MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		QuantileDiscretizerTrainBatchOp qop = new QuantileDiscretizerTrainBatchOp()
			.setNumBuckets(3)
			.setSelectedCols(colnames[1], colnames[2])
			.linkFrom(sourceBatchOp);

		qop.lazyPrint(-1);

		BatchOperator.execute();

	}

	@Test
	public void testLazyPrint() throws Exception {
		RandomTableSourceBatchOp op = new RandomTableSourceBatchOp()
			.setIdCol("id")
			.setNumCols(40)
			.setNumRows(200L);

		QuantileDiscretizerTrainBatchOp qop = new QuantileDiscretizerTrainBatchOp()
			.setNumBuckets(20)
			.setSelectedCols(ArrayUtils.removeElements(op.getColNames(), "id"))
			.linkFrom(op);

		qop.lazyCollectModelInfo(new Consumer <QuantileDiscretizerModelInfo>() {
			@Override
			public void accept(
				QuantileDiscretizerModelInfo quantileDiscretizerModelInfo) {
				for (String s : quantileDiscretizerModelInfo.getSelectedColsInModel()) {
					System.out.println(s + ":" + JsonConverter.toJson(quantileDiscretizerModelInfo.getCutsArray(s)));
				}
			}
		});

		qop.lazyPrintModelInfo();

		BatchOperator.execute();

	}
}