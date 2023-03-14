package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VertexClusterCoefficientBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 2),
			Row.of(1, 3),
			Row.of(1, 4),
			Row.of(1, 5),
			Row.of(1, 6),
			Row.of(2, 3),
			Row.of(4, 3),
			Row.of(5, 4),
			Row.of(5, 6),
			Row.of(5, 7),
			Row.of(6, 7)

		};
		DataSet <Row> dataSet = MLEnvironmentFactory.getDefault().getExecutionEnvironment().fromCollection(
			Arrays.asList(rows));
		BatchOperator inData = new TableSourceBatchOp(DataSetConversionUtil
			.toTable(0L, dataSet, new String[] {"source", "target"},
				new TypeInformation[] {Types.BIG_INT, Types.BIG_INT}));
		VertexClusterCoefficientBatchOp op = new VertexClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(inData);
//		op.print();
		List<Row> listRes = op.collect();
		Assert.assertEquals(listRes.size(), 7);
		new VertexClusterCoefficientBatchOp(new Params());
	}

}