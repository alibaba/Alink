package com.alibaba.alink.pipeline;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.OneVsRest;
import com.alibaba.alink.pipeline.clustering.Lda;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizer;
import com.alibaba.alink.pipeline.regression.GeneralizedLinearRegression;
import com.alibaba.alink.pipeline.sql.Select;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LocalPredictorTest extends LocalPredictorMultThreadTestBase {

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

		Select select = new Select()
			.setClause("cast(sepal_length as double) as sepal_length, "
				+ "cast(petal_width as double) as petal_width, "
				+ "cast(petal_length as double) as petal_length, "
				+ "category"
			);

		//Glm
		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFeatureCols("sepal_length", "petal_width")
			.setLabelCol("petal_length")
			.setPredictionCol("glm_pred");

		//linear
		//RandomForestClassifier gbdtClassifier = new RandomForestClassifier()
		//	.setFeatureCols("sepal_length", "petal_width")
		//	.setLabelCol("category")
		//	.setPredictionCol("gbdt_pred");

		return new Pipeline()
			.add(binarizer)
			.add(assembler)
			//.add(binarizer)
			//.add(assembler)
			//.add(binarizer)
			//.add(assembler)
			//.add(binarizer)
			//.add(assembler)
			//.add(binarizer)
			//.add(assembler)
			//.add(binarizer)
			//.add(assembler)
			//.add(binarizer)
			//.add(assembler)
			.add(quantileDiscretizer)
			//.add(quantileDiscretizer)
			//.add(quantileDiscretizer)
			//.add(quantileDiscretizer)
			//.add(select)
			.add(glm)
			//.add(lda)
			//.add(gbdtClassifier)
			;
	}

	protected BatchOperator getTrainSource() {
		return new CsvSourceBatchOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv");
	}

	@Test
	public void testGeneModelStream() throws Exception {
		BatchOperator data = Iris.getBatchData();

		LogisticRegression lr = new LogisticRegression()
			.setFeatureCols(Iris.getFeatureColNames())
			.setLabelCol(Iris.getLabelColName())
			.setPredictionCol("pred_label")
			.setPredictionDetailCol("pred_detail")
			.setModelStreamFilePath("/tmp/rankModel")
			.setMaxIter(100);

		OneVsRest oneVsRest = new OneVsRest()
			.setClassifier(lr)
			.setNumClass(3)
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail");

		VectorAssembler va = new VectorAssembler()
			.setSelectedCols("sepal_length", "sepal_width")
			.setOutputCol("assem");

		Pipeline pipeline = new Pipeline()
			.add(oneVsRest)
			.add(va);

		PipelineModel model = pipeline.fit(data);
		model.save().link(new AkSinkBatchOp()
			.setFilePath("/tmp/rankModel.ak")
			.setOverwriteSink(true));
		BatchOperator.execute();
	}


}
