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

public class EdgeClusterCoefficientBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		//the input edge construct the directed graph,
		//while the EdgeClusterCoefficient algorithm requires undirected graph.
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.0, 2.0, 0.1}),
			Row.of(new Object[] {1.0, 3.0, 0.1}),
			Row.of(new Object[] {3.0, 2.0, 0.1}),
			Row.of(new Object[] {5.0, 2.0, 0.1}),
			Row.of(new Object[] {3.0, 4.0, 0.1}),
			Row.of(new Object[] {4.0, 2.0, 0.1}),
			Row.of(new Object[] {5.0, 4.0, 0.1}),
			Row.of(new Object[] {5.0, 1.0, 0.1}),
			Row.of(new Object[] {5.0, 3.0, 0.1}),
			Row.of(new Object[] {5.0, 6.0, 0.1}),
			Row.of(new Object[] {5.0, 8.0, 0.1}),
			Row.of(new Object[] {7.0, 6.0, 0.1}),
			Row.of(new Object[] {7.0, 1.0, 0.1}),
			Row.of(new Object[] {7.0, 5.0, 0.1}),
			Row.of(new Object[] {8.0, 6.0, 0.1}),
			Row.of(new Object[] {8.0, 4.0, 0.1})};

		DataSet <Row> dataSet = MLEnvironmentFactory.getDefault().getExecutionEnvironment().fromCollection(
			Arrays.asList(rows));
		BatchOperator inData = new TableSourceBatchOp(DataSetConversionUtil
			.toTable(0L, dataSet, new String[] {"source", "target", "weight"},
				new TypeInformation[] {Types.BIG_INT, Types.BIG_INT, Types.FLOAT}));
		BatchOperator res = new EdgeClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(inData);
		List<Row> listRes = res.collect();
		Assert.assertEquals(listRes.size(), 16);
		new EdgeClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target").setAsUndirectedGraph(false)
			.linkFrom(inData).lazyCollect();
		new EdgeClusterCoefficientBatchOp(new Params());
		BatchOperator.execute();
	}
}