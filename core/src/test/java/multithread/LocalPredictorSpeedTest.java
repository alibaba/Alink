package multithread;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.clustering.Lda;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizer;
import com.alibaba.alink.pipeline.regression.GeneralizedLinearRegression;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LocalPredictorSpeedTest {

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

	protected Pipeline getPipeline() {
		//model mapper
		QuantileDiscretizer quantileDiscretizer = new QuantileDiscretizer()
			.setNumBuckets(2)
			.setSelectedCols("sepal_length");

		//SISO mapper
		Binarizer binarizer = new Binarizer()
			.setSelectedCol("petal_width")
			.setOutputCol("bina")
			.setReservedCols("sepal_length", "petal_width", "petal_length", "category")
			.setThreshold(1.);

		//MISO Mapper
		VectorAssembler assembler = new VectorAssembler()
			.setSelectedCols("sepal_length", "petal_width")
			.setOutputCol("assem")
			.setReservedCols("sepal_length", "petal_width", "petal_length", "category");

		//Lda
		Lda lda = new Lda()
			.setPredictionCol("lda_pred")
			.setPredictionDetailCol("lda_pred_detail")
			.setSelectedCol("category")
			.setTopicNum(2)
			.setRandomSeed(0);


		//Glm
		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFeatureCols("sepal_length", "petal_width")
			.setLabelCol("petal_length")
			.setPredictionCol("glm_pred");


		return new Pipeline()
			.add(binarizer)
			.add(assembler)
			.add(binarizer)
			.add(assembler)
			.add(binarizer)
			.add(assembler)
			.add(binarizer)
			.add(assembler)
			.add(binarizer)
			.add(assembler)
			.add(binarizer)
			.add(assembler)
			//.add(quantileDiscretizer)
			//.add(lda)
			;
	}

	protected BatchOperator getTrainSource() {
		return new CsvSourceBatchOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");
	}

	@Test
	public void test2() throws Exception {
		BatchOperator source = getTrainSource();

		Pipeline pipeline = getPipeline();

		PipelineModel model = pipeline.fit(source);

		LocalPredictor predictor = model.collectLocalPredictor(source.getSchema());

		long start = System.currentTimeMillis();

		int n = 10000000;
		for(int i = 0; i<n; i++) {
			predictor.map(Row.of(5.1000, 3.5000, 1.4000, 0.2000, "Iris-setosa"));
		}

		long end = System.currentTimeMillis();

		System.out.println("end-start: " + (end-start));

	}

}
