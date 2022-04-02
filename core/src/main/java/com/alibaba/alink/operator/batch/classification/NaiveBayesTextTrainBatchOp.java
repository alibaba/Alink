package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelData;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelDataConverter;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;

import java.util.ArrayList;

/**
 * Text Naive Bayes Classifier.
 *
 * We support the multinomial Naive Bayes and bernoulli Naive Bayes model, a probabilistic learning method.
 * Here, the input data must be vector and the values must be nonnegative.
 *
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "weightCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("朴素贝叶斯文本分类训练")
public class NaiveBayesTextTrainBatchOp
	extends BatchOperator <NaiveBayesTextTrainBatchOp>
	implements NaiveBayesTextTrainParams <NaiveBayesTextTrainBatchOp>,
	WithModelInfoBatchOp <NaiveBayesTextModelInfo, NaiveBayesTextTrainBatchOp, NaiveBayesTextModelInfoBatchOp> {

	private static final long serialVersionUID = 1343509041059789517L;

	/**
	 * Constructor.
	 */
	public NaiveBayesTextTrainBatchOp() {
		super(new Params());
	}

	/**
	 * Constructor.
	 *
	 * @param params the parameters of the algorithm.
	 */
	public NaiveBayesTextTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public NaiveBayesTextTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		TypeInformation <?> labelType;
		String labelColName = getLabelCol();
		ModelType modelType = getModelType();
		String weightColName = getWeightCol();
		double smoothing = getSmoothing();
		String vectorColName = getVectorCol();

		labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);

		String[] keepColNames = (weightColName == null) ? new String[] {labelColName}
			: new String[] {weightColName, labelColName};
		Tuple2 <DataSet <Tuple2 <Vector, Row>>, DataSet <BaseVectorSummary>> dataSrt
			= StatisticsHelper.summaryHelper(in, null, vectorColName, keepColNames);
		DataSet <Tuple2 <Vector, Row>> data = dataSrt.f0;
		DataSet <BaseVectorSummary> srt = dataSrt.f1;

		DataSet <Integer> vectorSize = srt.map(new MapFunction <BaseVectorSummary, Integer>() {
			private static final long serialVersionUID = -4626037497952553113L;

			@Override
			public Integer map(BaseVectorSummary value) {
				return value.vectorSize();
			}
		});

		// Transform data in the form of label, weight, feature.
		DataSet <Tuple3 <Object, Double, Vector>> trainData = data
			.mapPartition(new Transform());
		DataSet <Tuple3 <Object, Double, Vector>> labelFeatureInfo = trainData
			.groupBy(new SelectLabel())
			.reduceGroup(new ReduceItem())
			.withBroadcastSet(vectorSize, "vectorSize");

		String[] featureCols = null;
		if (getParams().contains(HasFeatureCols.FEATURE_COLS)) {
			featureCols = getParams().get(HasFeatureCols.FEATURE_COLS);
		}
		DataSet <Row> probs = labelFeatureInfo
			.mapPartition(new GenerateModel(smoothing, modelType, vectorColName, featureCols, labelType))
			.withBroadcastSet(vectorSize, "vectorSize")
			.setParallelism(1);

		//save the model matrix.
		this.setOutput(probs, new NaiveBayesTextModelDataConverter(labelType).getModelSchema());
		return this;
	}

	/**
	 * Generate model.
	 */
	public static class GenerateModel extends AbstractRichFunction
		implements MapPartitionFunction <Tuple3 <Object, Double, Vector>, Row> {
		private static final long serialVersionUID = -8763129628694985528L;
		private int numFeature;
		private double smoothing;
		private ModelType modelType;
		private String vectorColName;
		private String[] featureCols;
		private TypeInformation labelType;

		GenerateModel(double smoothing, ModelType modelType, String vectorColName, String[] featureCols,
					  TypeInformation labelType) {
			this.smoothing = smoothing;
			this.modelType = modelType;
			this.labelType = labelType;
			this.vectorColName = vectorColName;
			this.featureCols = featureCols;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Object, Double, Vector>> values, Collector <Row> collector)
			throws Exception {
			double numDocs = 0.0;
			ArrayList <Tuple3 <Object, Double, DenseVector>> modelArray = new ArrayList <>();

			for (Tuple3 <Object, Double, Vector> tuple3 : values) {
				numDocs += tuple3.f1;
				modelArray.add(Tuple3.of(tuple3.f0, tuple3.f1, (DenseVector) tuple3.f2));
			}
			int numLabels = modelArray.size();
			double piLog = Math.log(numDocs + numLabels * this.smoothing);

			DenseMatrix theta = new DenseMatrix(numLabels, numFeature);
			double[] piArray = new double[numLabels];
			Object[] labels = new Object[numLabels];
			for (int i = 0; i < numLabels; ++i) {
				DenseVector feature = modelArray.get(i).f2;

				double thetaLog = 0.0;
				switch (this.modelType) {
					case Multinomial: {
						double numTerm = 0.0;
						for (int j = 0; j < feature.size(); ++j) {
							numTerm += feature.get(j);
						}
						thetaLog += Math.log(numTerm + this.numFeature * this.smoothing);
						break;
					}
					case Bernoulli: {
						thetaLog += Math.log(modelArray.get(i).f1 + 2.0 * this.smoothing);
						break;
					}
					default: {
						break;
					}
				}
				labels[i] = modelArray.get(i).f0;
				piArray[i] = Math.log(modelArray.get(i).f1 + this.smoothing) - piLog;

				for (int j = 0; j < feature.size(); ++j) {
					theta.set(i, j, Math.log(feature.get(j) + this.smoothing) - thetaLog);
				}
			}

			NaiveBayesTextModelData trainResultData = new NaiveBayesTextModelData();
			trainResultData.pi = piArray;
			trainResultData.labels = labels;
			trainResultData.theta = theta;
			trainResultData.vectorColName = vectorColName;
			trainResultData.modelType = modelType;
			trainResultData.featureColNames = featureCols;
			trainResultData.vectorSize = modelArray.get(0).f2.size();

			new NaiveBayesTextModelDataConverter(labelType).save(trainResultData, collector);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.numFeature = (Integer) getRuntimeContext()
				.getBroadcastVariable("vectorSize").get(0);
		}
	}

	/**
	 * Transform the data format.
	 */
	public static class Transform
		implements MapPartitionFunction <Tuple2 <Vector, Row>, Tuple3 <Object, Double, Vector>> {

		private static final long serialVersionUID = -1962725988758297056L;

		@Override
		public void mapPartition(Iterable <Tuple2 <Vector, Row>> values,
								 Collector <Tuple3 <Object, Double, Vector>> out)
			throws Exception {
			for (Tuple2 <Vector, Row> in : values) {
				Vector feature = in.f0;
				Object labelVal = in.f1.getArity() == 2 ? in.f1.getField(1) : in.f1.getField(0);
				Double weightVal = in.f1.getArity() == 2 ?
					in.f1.getField(0) instanceof Number ?
						((Number) in.f1.getField(0)).doubleValue() :
						Double.parseDouble(in.f1.getField(0).toString())
					: 1.0;
				out.collect(new Tuple3 <>(labelVal, weightVal, feature));

			}
		}
	}

	/**
	 * Group by trainData with its label.
	 */
	public static class SelectLabel implements KeySelector <Tuple3 <Object, Double, Vector>, String> {

		private static final long serialVersionUID = -3406893536674801451L;

		@Override
		public String getKey(Tuple3 <Object, Double, Vector> t3) {
			return t3.f0.toString();
		}
	}

	/**
	 * Calculate the sum of feature with same label and the label weight.
	 */
	public static class ReduceItem extends AbstractRichFunction
		implements GroupReduceFunction <Tuple3 <Object, Double, Vector>, Tuple3 <Object, Double, Vector>> {
		private static final long serialVersionUID = -3644173529201603819L;
		private int vectorSize = 0;

		@Override
		public void reduce(Iterable <Tuple3 <Object, Double, Vector>> rows,
						   Collector <Tuple3 <Object, Double, Vector>> out) {
			Object label = null;

			double weightSum = 0.0;
			Vector featureSum = new DenseVector(this.vectorSize);

			for (Tuple3 <Object, Double, Vector> row : rows) {
				label = row.f0;
				double w = row.f1;
				weightSum += w;
				if (row.f2 instanceof SparseVector) {
					((SparseVector) row.f2).setSize(this.vectorSize);
					int[] idx = ((SparseVector) row.f2).getIndices();
					double[] val = ((SparseVector) row.f2).getValues();
					for (int i = 0; i < idx.length; ++i) {
						featureSum.add(idx[i], val[i] * w);
					}
				} else {
					for (int i = 0; i < this.vectorSize; ++i) {
						featureSum.set(i, featureSum.get(i) + row.f2.get(i) * w);
					}
				}
			}
			Tuple3 <Object, Double, Vector> t3 = new Tuple3 <>(label, weightSum, featureSum);

			out.collect(t3);
		}

		@Override
		public void open(Configuration parameters) throws Exception {

			this.vectorSize = (Integer) getRuntimeContext()
				.getBroadcastVariable("vectorSize").get(0);
		}

	}

	@Override
	public NaiveBayesTextModelInfoBatchOp getModelInfoBatchOp() {
		return new NaiveBayesTextModelInfoBatchOp(getParams()).linkFrom(this);
	}

}
