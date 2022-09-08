package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TreeDepthBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		ArrayList <Row> edges = new ArrayList <Row>();
		edges.add(Row.of(0, 1, 10.));
		edges.add(Row.of(0, 2, 1.));
		edges.add(Row.of(1, 3, 1.));
		edges.add(Row.of(1, 4, 2.));
		edges.add(Row.of(2, 5, 1.));
		edges.add(Row.of(4, 6, 1.));
		edges.add(Row.of(7, 8, 1.));
		edges.add(Row.of(7, 9, 1.));
		edges.add(Row.of(9, 10, 1.));
		edges.add(Row.of(9, 11, 1.));
		DataSet <Row> dataSet = MLEnvironmentFactory.getDefault().getExecutionEnvironment().fromCollection(edges);
		BatchOperator inData = new TableSourceBatchOp(DataSetConversionUtil
			.toTable(0L, dataSet, new String[] {"source", "target", "weight"},
				new TypeInformation[] {Types.INT, Types.INT, Types.FLOAT}));
		TreeDepthBatchOp treeOp = new TreeDepthBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.linkFrom(inData);
		treeOp.print();
		List<Row> listRes = treeOp.collect();
		Assert.assertEquals(listRes.size(), 12);
		new TreeDepthBatchOp(null);
	}
}