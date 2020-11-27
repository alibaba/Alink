package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class StreamingKMeansStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1 0.1 0.1"),
			Row.of(2, "0.2 0.2 0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		};

		Row[] rows1 = new Row[] {
			Row.of(0, "1 1 1"),
			Row.of(1, "2 2 2"),
			Row.of(2, "3 3 3"),
			Row.of(3, "10 10 10"),
			Row.of(4, "11 11 11"),
			Row.of(5, "12 12 12")
		};

		TableSchema tableSchema = new TableSchema(new String[] {"id", "vec"},
			new TypeInformation[] {Types.INT, VectorTypes.VECTOR});

		MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(Arrays.asList(rows), tableSchema);

		Row[] predict = new Row[200000];
		for (int i = 0; i < predict.length; i++) {
			predict[i] = Row.of(DenseVector.rand(3));
		}

		MemSourceStreamOp predictOp = new MemSourceStreamOp(Arrays.asList(predict), new String[] {"vec"});

		KMeansTrainBatchOp trainBatchOp = new KMeansTrainBatchOp()
			.setVectorCol("vec")
			.setK(2);

		BatchOperator model = trainBatchOp.linkFrom(sourceBatchOp);

		MemSourceStreamOp sourceStreamOp = new MemSourceStreamOp(Arrays.asList(rows1), tableSchema);

		StreamingKMeansStreamOp op = new StreamingKMeansStreamOp(model)
			.setPredictionCol("pred")
			.setTimeInterval(1L)
			.setHalfLife(1)
			.setReservedCols("vec");

		StreamOperator res = op.linkFrom(predictOp, predictOp);

		res.getSideOutput(0).print();

		//res.print();

		StreamOperator.execute();
	}
}