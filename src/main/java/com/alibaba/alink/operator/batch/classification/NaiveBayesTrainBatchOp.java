package com.alibaba.alink.operator.batch.classification;

import java.util.ArrayList;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter;
import com.alibaba.alink.operator.common.classification.NaiveBayesTrainModelData;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.classification.NaiveBayesTrainParams;

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
import org.apache.flink.util.Preconditions;

/**
 * Naive Bayes Classifier.
 *
 * We support the multinomial Naive Bayes and multinomial Naive Bayes model, a probabilistic learning method.
 * Here, feature values of train table must be nonnegative.
 */

public final class NaiveBayesTrainBatchOp
	extends BatchOperator<NaiveBayesTrainBatchOp>
	implements NaiveBayesTrainParams <NaiveBayesTrainBatchOp> {

	/**
	 * Constructor.
	 */
	public NaiveBayesTrainBatchOp() {
		super(new Params());
	}

	/**
	 * Constructor.
	 *
	 * @param params the parameters of the algorithm.
	 */
	public NaiveBayesTrainBatchOp(Params params) {
		super(params);
	}

	/**
	 * Train data and get a model.
	 *
	 * @param inputs input data.
	 * @return the model of naive bayes.
	 */
	@Override
	public NaiveBayesTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		TypeInformation <?> labelType;
		String[] featureColNames = getFeatureCols();
		String labelColName = getLabelCol();
		NaiveBayesModelDataConverter.BayesType bayesType = NaiveBayesModelDataConverter.BayesType
				.valueOf(getModelType().toUpperCase());
		String weightColName = getWeightCol();
		double smoothing = getSmoothing();
		String vectorColName = getVectorCol();
		String[] colNames = in.getColNames();

		Integer[] featIdx;
		int featureLength;

		if (featureColNames != null) {
			featureLength = featureColNames.length;
			featIdx = new Integer[featureLength];
			for (int i = 0; i < featureLength; ++i) {
				featIdx[i] = TableUtil.findColIndex(colNames, featureColNames[i]);
				TypeInformation type = in.getSchema().getFieldTypes()[featIdx[i]];

				Preconditions.checkArgument(TableUtil.isNumber(type),
						"naive bayes algorithm only support numerical data type.");
			}
		} else if (vectorColName != null) {
			featIdx = new Integer[1];
			featIdx[0] = TableUtil.findColIndex(colNames, vectorColName);
		} else {
			throw new IllegalArgumentException("feature col info error.");
		}

		labelType = in.getColTypes()[TableUtil.findColIndex(in.getColNames(), labelColName)];

		String[] keepColNames = (weightColName == null) ? new String[] {labelColName}
			: new String[] {weightColName, labelColName};
		Tuple2 <DataSet <Tuple2 <Vector, Row>>, DataSet <BaseVectorSummary>> dataSrt
			= StatisticsHelper.summaryHelper(in, featureColNames, vectorColName, keepColNames);
		DataSet <Tuple2 <Vector, Row>> data = dataSrt.f0;
		DataSet <BaseVectorSummary> srt = dataSrt.f1;

		DataSet <Integer> vectorSize = srt.map(new MapFunction <BaseVectorSummary, Integer>() {
			@Override
			public Integer map(BaseVectorSummary value) {
				return value.vectorSize();
			}
		});

		// Transform data in the form of label, weight, feature.
		DataSet <Tuple3 <Object, Double, Vector>> trainData = data
			.mapPartition(new Transform());

		DataSet <Row> probs;
		probs = trainData.groupBy(new SelectLabel())
			.reduceGroup(new ReduceItem())
			.withBroadcastSet(vectorSize, "vectorSize")
			.mapPartition(new GenerateModel(smoothing, bayesType, featureColNames, labelType))
			.withBroadcastSet(vectorSize, "vectorSize")
			.setParallelism(1);

        //save the model matrix.
		this.setOutput(probs, new NaiveBayesModelDataConverter(labelType).getModelSchema());
		return this;
	}

	/**
	 * Generate model.
	 */
	public static class GenerateModel extends AbstractRichFunction
		implements MapPartitionFunction <Tuple3 <Object, Double, Vector>, Row> {
		private int numFeature;
		private double smoothing;
		private NaiveBayesModelDataConverter.BayesType bayesType;
		private String[] featureColNames;
		private TypeInformation labelType;

		GenerateModel(double smoothing, NaiveBayesModelDataConverter.BayesType bayesType,
					  String[] featureColNames, TypeInformation labelType) {
			this.smoothing = smoothing;
			this.bayesType = bayesType;
			this.labelType = labelType;
			this.featureColNames = featureColNames;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Object, Double, Vector>> values, Collector <Row> collector)
			throws Exception {
			double numDocs = 0.0;
			ArrayList <Tuple3 <Object, Double, Vector>> modelArray = new ArrayList <>();

			for (Tuple3 <Object, Double, Vector> tup : values) {
				numDocs += tup.f1;
				modelArray.add(tup);
			}
			int numLabels = modelArray.size();
			double piLog = Math.log(numDocs + numLabels * this.smoothing);

			DenseMatrix theta = new DenseMatrix(numLabels, numFeature);
			double[] piArray = new double[numLabels];
			Object[] labels = new Object[numLabels];
			for (int i = 0; i < numLabels; ++i) {
				DenseVector feature = (DenseVector) modelArray.get(i).f2;
				double numTerm = 0.0;
				for (int j = 0; j < feature.size(); ++j) {
					numTerm += feature.get(j);
				}
				double thetaLog = 0.0;
				switch (this.bayesType) {
					case MULTINOMIAL: {
						thetaLog += Math.log(numTerm + this.numFeature * this.smoothing);
						break;
					}
					case BERNOULLI: {
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

			NaiveBayesTrainModelData trainResultData = new NaiveBayesTrainModelData();
			trainResultData.pi = piArray;
			trainResultData.label = labels;
			trainResultData.theta = theta;
			trainResultData.featureNames = featureColNames;
			trainResultData.modelType = bayesType;

			new NaiveBayesModelDataConverter(labelType).save(trainResultData, collector);
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

}
