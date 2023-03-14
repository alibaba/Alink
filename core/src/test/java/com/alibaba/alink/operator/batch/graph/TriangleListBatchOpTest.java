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

public class TriangleListBatchOpTest extends AlinkTestBase {
	@Test
	public void testTriangleList() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1.0, 2.0),
			Row.of(1.0, 3.0),
			Row.of(1.0, 4.0),
			Row.of(1.0, 5.0),
			Row.of(1.0, 6.0),
			Row.of(2.0, 3.0),
			Row.of(4.0, 3.0),
			Row.of(5.0, 4.0),
			Row.of(5.0, 6.0),
			Row.of(5.0, 7.0),
			Row.of(6.0, 7.0)
		};
		DataSet <Row> dataSet = MLEnvironmentFactory.getDefault().getExecutionEnvironment().fromCollection(
			Arrays.asList(rows));
		BatchOperator inData = new TableSourceBatchOp(DataSetConversionUtil
			.toTable(0L, dataSet, new String[] {"source", "target"}, new TypeInformation[] {Types.INT, Types.INT}));

		TriangleListBatchOp op = new TriangleListBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target");

		BatchOperator res = op.linkFrom(inData)
			.select(new String[] {"node1", "node2", "node3"});

		List<Row> listRes =  res.collect();
		Assert.assertEquals(listRes.size(), 5);
		new TriangleListBatchOp(new Params());
	}

}