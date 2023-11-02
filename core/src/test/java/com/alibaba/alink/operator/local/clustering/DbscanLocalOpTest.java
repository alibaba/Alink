package com.alibaba.alink.operator.local.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.params.shared.clustering.HasFastMetric.Metric;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

//public class DbscanLocalOpTest extends AlinkTestBase {
public class DbscanLocalOpTest{
	@Test
	public void test1() {
		int sampleNum = 1000;
		int dim = 20;

		Row[] data = new Row[sampleNum];
		Random random = new Random();
		for (int i = 0; i < sampleNum; i++) {
			double[] values = new double[dim];
			for (int j = 0; j < dim; j++) {
				values[j] = random.nextDouble();
			}
			data[i] = Row.of(new DenseVector(values), (long)i);
		}

		LocalOperator source = new MemSourceLocalOp(new MTable(data, "vec vector, id long"));

		LocalOperator result = new DbscanLocalOp()
			.setNumThreads(4)
			.setIdCol("id")
			.setPredictionCol("label")
			.setSelectedCol("vec")
			.setMetric(Metric.EUCLIDEAN)
			.setRadius(0.1)
			.setTopN(5)
			.linkFrom(source);

		LocalOperator model = new DbscanTrainLocalOp()
			.setSelectedCol("vec")
			.setIdCol("id")
			.setNumThreads(4)
			.setMetric(Metric.EUCLIDEAN)
			.setRadius(0.1)
			.setTopN(5)
			.linkFrom(source);

		LocalOperator predict = new DbscanPredictLocalOp()
			.setSelectedCol("vec")
			.setPredictionCol("label")
			.setNumThreads(4)
			.linkFrom(model, source)
			.print();

		MTable DbscanLocalOutTable = result.select("label").getOutputTable();
		MTable DbscanPredictOutTable = predict.select("label").getOutputTable();

		for (int i = 0; i < sampleNum; i++) {
			Assert.assertEquals(DbscanLocalOutTable.getRow(i).getField(0),
				DbscanPredictOutTable.getRow(i).getField(0));
		}
	}
}
