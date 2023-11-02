package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import com.alibaba.alink.params.shared.clustering.HasFastMetric.Metric;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Random;

public class VectorNearestNeighborTrainLocalOpTest extends TestCase {

	public void test() throws Exception {
		LocalOperator.setParallelism(4);
		int dim = 256;
		//int nDict = 120000;
		//int nQuery = 5000;
		int nDict = 3;
		int nQuery = 1;

		ArrayList <Row> listDict = new ArrayList <>();
		ArrayList <Row> listQuery = new ArrayList <>();

		DenseVector ones = DenseVector.ones(dim);
		for (int i = 0; i < nDict; i++) {
			listDict.add(Row.of(i, ones.scale(i)));
		}
		Random rand = new Random(123);
		for (int i = 0; i < nQuery; i++) {
			listQuery.add(Row.of(i, ones.scale(Math.abs(rand.nextInt(nDict)))));
		}

		LocalOperator <?> model = new MemSourceLocalOp(listDict, "id int, vec vector")
			.link(
				new VectorNearestNeighborTrainLocalOp()
					.setIdCol("id")
					.setSelectedCol("vec")
					//.setMetric(Metric.COSINE)
					.setMetric(Metric.EUCLIDEAN)
			);

		LocalOperator <?> query = new TableSourceLocalOp(new MTable(listQuery, "id int, vec vector"));

		boolean useFastMode = true;
		if (useFastMode) {
			new VectorNearestNeighborPredictLocalOp()
				.setNumThreads(4)
				.setSelectedCol("vec")
				.setOutputCol("topN")
				.setTopN(3)
				.linkFrom(model, query)
				.print(10);
		} else {
			new VectorNearestNeighborPredictLocalOp.SubVectorNearestNeighborPredictLocalOp()
				.setNumThreads(4)
				.setSelectedCol("vec")
				.setOutputCol("topN")
				.setTopN(3)
				.linkFrom(model, query)
				.print(10);
		}
	}

}