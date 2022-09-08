package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LineBatchOpTest extends AlinkTestBase {

	public static BatchOperator generateData(boolean getReversed) {
		Row[] rOrigin = new Row[] {
			Row.of("1L", "5L", 1.),
			Row.of("2L", "5L", 1.),
			Row.of("3L", "5L", 1.),
			Row.of("4L", "5L", 1.),
			Row.of("1L", "6L", 1.),
			Row.of("2L", "6L", 1.),
			Row.of("3L", "6L", 1.),
			Row.of("4L", "6L", 1.),
			Row.of("7L", "6L", 15.),
			Row.of("7L", "8L", 1.),
			Row.of("7L", "9L", 1.),
			Row.of("7L", "10L", 1.),
		};
		Row[] r;
		if (getReversed) {
			int size = rOrigin.length;
			r = new Row[size * 2];
			for (int i = 0; i < size; i++) {
				Row r1 = rOrigin[i];
				r[2 * i] = r1;
				Row r2 = Row.copy(r1);
				Object temp = r1.getField(0);
				r2.setField(0, r2.getField(1));
				r2.setField(1, temp);
				r[2 * i + 1] = r2;
			}
		} else {
			r = rOrigin;
		}
		DataSet <Row> dataSet = MLEnvironmentFactory.getDefault()
			.getExecutionEnvironment().fromCollection(Arrays.asList(r));
		BatchOperator inData = new TableSourceBatchOp(DataSetConversionUtil
			.toTable(0L, dataSet, new String[] {"source", "target", "weight"},
				new TypeInformation[] {Types.STRING, Types.STRING, Types.DOUBLE}));
		return inData;
	}

	@Test
	public void testHuge() throws Exception {
		BatchOperator data = LineBatchOpTest.generateData(true);
		LineBatchOp line = new LineBatchOp().setOrder("secondOrder").setRho(.025)
			.setVectorSize(5).setNegative(5).setIsToUndigraph(false).setMaxIter(20).setSampleRatioPerPartition(2.)
			.setSourceCol("source").setTargetCol("target").setWeightCol("weight")
			.setBatchSize(5).setMinRhoRate(0.001).setRho(0.03);
		BatchOperator res = data.link(line);
		res.select(new String[] {"vertexId"}).lazyCollect();
		BatchOperator res2 = new LineBatchOp().setOrder("firstOrder").setRho(.025)
			.setVectorSize(5).setNegative(5).setIsToUndigraph(true).setMaxIter(2).setSampleRatioPerPartition(0.5)
			.setSourceCol("source").setTargetCol("target").linkFrom(data);
		List<Row> listRes = res2.collect();
		Assert.assertEquals(listRes.size(), 10);
		new LineBatchOp(new Params());

	}

}