package multithread;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.probabilistic.XRandom;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public abstract class LocalPredictorMultThreadTestBase2 {

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
		int threadNum = 3;
		List <Row> inRows = getInputRows();
		Pipeline pipeline = getPipeline();
		BatchOperator trainSource = getTrainSource();
		PipelineModel pipelineModel = pipeline.fit(trainSource);

		LocalPredictor predictor = pipelineModel.collectLocalPredictor(trainSource.getSchema());

		List <Row> outRows = new ArrayList <>();
		for (Row inRow : inRows) {
			outRows.add(predictor.map(inRow));
		}

		MyThread[] threads = new MyThread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new MyThread(predictor, inRows, outRows);
			threads[i].start();
		}

		for (int i = 0; i < threadNum; i++) {
			threads[i].join();

		}

	}

	private static class MyThread extends Thread {
		private LocalPredictor localPredictor;
		private List <Row> inRows;
		private List <Row> outRows;
		private int n = 10000000 ;

		MyThread(LocalPredictor localPredictor, List <Row> inRows, List <Row> outRows) {
			this.localPredictor = localPredictor;
			this.inRows = inRows;
			this.outRows = outRows;
		}

		public void run() {
			Row row = inRows.get(0);
			long start = System.currentTimeMillis();
			try {
				for (int i = 0; i < n; i++) {

					localPredictor.map(row);

				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			System.out.println(
				"threadId: " + Thread.currentThread().getId() + " time: " + (System.currentTimeMillis() - start));

		}


	}

}
