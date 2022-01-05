package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VectorImputerTest extends AlinkTestBase {

	public static AlgoOperator getData(boolean isBatch) {
		Row[] rowData =
			new Row[] {
				Row.of("0", "1, 3, NaN"),
				Row.of("1", "0:-1.0 1:-3.0"),
				Row.of("2", "0:4.0 1:2.0")
			};

		TableSchema schema = new TableSchema(
			new String[] {"id", "vec"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING}
		);
		if (isBatch) {
			BatchOperator batchData = new MemSourceBatchOp(Arrays.asList(rowData), schema);
			return batchData;
		} else {
			StreamOperator streamData = new MemSourceStreamOp(Arrays.asList(rowData), schema);
			return streamData;
		}
	}

	@Test
	public void testPipelineFillValue() throws Exception {
		String selectedColName = "vec";
		String strategy = "value";
		double fillValue = -7.0;

		BatchOperator batchData = (BatchOperator) getData(true);
		StreamOperator streamData = (StreamOperator) getData(false);
		VectorImputer fillMissingValue = new VectorImputer()
			.setSelectedCol(selectedColName)
			.setStrategy(strategy)
			.setFillValue(fillValue);

		VectorImputerModel model = fillMissingValue.fit(batchData);

		BatchOperator res = model.transform(batchData);
		List <Row> rows = res.getDataSet().collect();
		rows.sort(new RowComparator(0));

		assertEquals(VectorUtil.getVector(rows.get(0).getField(1)),
			VectorUtil.getVector("1.0 3.0 -7.0"));
		assertEquals(VectorUtil.getVector(rows.get(1).getField(1)),
			VectorUtil.getVector("0:-1.0 1:-3.0"));
		assertEquals(VectorUtil.getVector(rows.get(2).getField(1)),
			VectorUtil.getVector("0:4.0 1:2.0"));

		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp().linkFrom(model.transform(streamData));
		StreamOperator.execute();
		List <Row> result = collectSinkStreamOp.getAndRemoveValues();
		result.sort(new RowComparator(0));
		assertEquals(VectorUtil.getVector(result.get(0).getField(1)),
			VectorUtil.getVector("1.0 3.0 -7.0"));
		assertEquals(VectorUtil.getVector(result.get(1).getField(1)),
			VectorUtil.getVector("0:-1.0 1:-3.0"));
		assertEquals(VectorUtil.getVector(result.get(2).getField(1)),
			VectorUtil.getVector("0:4.0 1:2.0"));
	}

	@Test
	public void testBatch() throws Exception {
		BatchOperator data = (BatchOperator) getData(true);
		String selectedColName = "vec";

		String strategy = "min";
		double fillValue = -7.0;
		Params params = new Params();
		params.set(VectorImputerTrainParams.STRATEGY,
			ParamUtil.searchEnum(VectorImputerTrainParams.STRATEGY, strategy));
		VectorImputerTrainBatchOp model = new VectorImputerTrainBatchOp(params)
			.setFillValue(fillValue).setSelectedCol(selectedColName).linkFrom(data);
		model.lazyPrint(-1);
		model.lazyPrintModelInfo();
		VectorImputerPredictBatchOp predict = new VectorImputerPredictBatchOp().setOutputCol("res");
		predict.linkFrom(model, data).print();
	}
}
