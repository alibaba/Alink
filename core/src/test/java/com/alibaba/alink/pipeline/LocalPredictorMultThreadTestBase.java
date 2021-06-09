package com.alibaba.alink.pipeline;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.probabilistic.XRandom;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public abstract class LocalPredictorMultThreadTestBase {

	protected List <Row> getInputRows() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(5.1000, 3.5000, 1.4000, 0.2000, "Iris-setosa"));
		rows.add(Row.of(5.0000, 2.0000, 3.5000, 1.0000, "Iris-versicolor"));
		rows.add(Row.of(5.1000, 3.7000, 1.5000, 0.4000, "Iris-setosa"));
		rows.add(Row.of(6.4000, 2.8000, 5.6000, 2.2000, "Iris-virginica"));
		rows.add(Row.of(6.0000, 2.9000, 4.5000, 1.5000, "Iris-versicolor"));
		rows.add(Row.of(4.9000, 3.0000, 1.4000, 0.2000, "Iris-setosa"));

		return rows;
	}

	protected abstract Pipeline getPipeline();

	protected abstract BatchOperator getTrainSource();

	@Test
	public void testMultiThread() throws Exception {
		int threadNum = 4;
		List <Row> inRows = getInputRows();
		Pipeline pipeline = getPipeline();
		BatchOperator trainSource = getTrainSource();
		PipelineModel pipelineModel = pipeline.fit(trainSource);

		LocalPredictor predictor = pipelineModel.collectLocalPredictor(trainSource.getSchema());

		List <Row> outRows = new ArrayList <>();
		for (Row inRow : inRows) {
			outRows.add(predictor.map(inRow));
		}

		long start = System.currentTimeMillis();

		MyThread[] threads = new MyThread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new MyThread(predictor, inRows, outRows);
			threads[i].start();
		}

		for (int i = 0; i < threadNum; i++) {
			threads[i].join();
		}

		System.out.println("time: " + (System.currentTimeMillis()-start));
	}

	private static class MyThread extends Thread {
		private LocalPredictor localPredictor;
		private List <Row> inRows;
		private List <Row> outRows;

		MyThread(LocalPredictor localPredictor, List <Row> inRows, List <Row> outRows) {
			this.localPredictor = localPredictor;
			this.inRows = inRows;
			this.outRows = outRows;
		}

		public void run() {
			int threadId = (int) Thread.currentThread().getId();

			int[] indices = new int[inRows.size()];
			for (int i = 0; i < indices.length; i++) {
				indices[i] = i;
			}
			shuffle(indices, threadId);

			for (int idx : indices) {
				Row expectRow = outRows.get(idx);
				Row calRow = null;
				try {
					calRow = localPredictor.map(inRows.get(idx));
					Assert.assertEquals(expectRow.toString(), calRow.toString());
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		}

		private void shuffle(int[] arr, int threadId) {
			XRandom random = new XRandom();
			random.setSeed(threadId);
			int length = arr.length;
			for (int i = length; i > 0; i--) {
				int randInd = random.nextInt(length);
				swap(arr, randInd, i - 1);
			}
		}

		private void swap(int[] a, int i, int j) {
			int temp = a[i];
			a[i] = a[j];
			a[j] = temp;
		}

	}

}
