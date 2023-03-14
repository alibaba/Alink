package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelData;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.classification.NaiveBayesTrainParams;
import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Naive Bayes classifier is a simple probability classification algorithm using
 * Bayes theorem based on independent assumption. It is an independent feature model.
 * The input feature can be continual or categorical.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "categoricalCols", allowedTypeCollections = TypeCollections.NAIVE_BAYES_CATEGORICAL_TYPES)
@ParamSelectColumnSpec(name = "featureCols")
@ParamSelectColumnSpec(name = "weightCol", allowedTypeCollections = TypeCollections.DOUBLE_TYPE)
@NameCn("朴素贝叶斯训练")
@NameEn("Naive Bayes Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.NaiveBayes")
public class NaiveBayesTrainBatchOp extends BatchOperator <NaiveBayesTrainBatchOp>
	implements NaiveBayesTrainParams <NaiveBayesTrainBatchOp>,
	WithModelInfoBatchOp <NaiveBayesModelInfo, NaiveBayesTrainBatchOp, NaiveBayesModelInfoBatchOp> {

	private static final long serialVersionUID = 8812570988530317418L;

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

	@Override
	public NaiveBayesTrainBatchOp linkFrom(BatchOperator <?>... inputs) {

		BatchOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = getLabelCol();
		TypeInformation <?> labelType = in.getColTypes()[TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
			labelColName)];
		String[] featureColNames = getFeatureCols();
		int featureSize = featureColNames.length;
		int[] featureIndices = TableUtil.findColIndices(in.getColNames(), featureColNames);
		TypeInformation <?>[] typeCols = new TypeInformation <?>[featureSize];
		for (int i = 0; i < featureSize; i++) {
			typeCols[i] = in.getColTypes()[featureIndices[i]];
		}
		String weightColName = getWeightCol() == null ? "" : getWeightCol();
		double smoothing = getSmoothing();
		String[] originalCategoricalCols = getParams().contains(HasCategoricalCols.CATEGORICAL_COLS) ?
			getCategoricalCols() : new String[0];
		boolean[] isCate = generateCategoricalCols(originalCategoricalCols,
			featureColNames, typeCols, labelColName, getParams());
		BatchOperator <?> stringIndexerModel = Preprocessing.generateStringIndexerModel(in, getParams());
		in = Preprocessing.castCategoricalCols(in, stringIndexerModel, getParams());
		in = Preprocessing.castContinuousCols(in, getParams());
		//continual columns don't have smooth factor and categorical columns have smooth factor.
		DataSet <Tuple3 <Object, Double, Number[]>> trainData = in.getDataSet()
			.mapPartition(new Transform(
				TableUtil.findColIndexWithAssertAndHint(in.getColNames(), labelColName),
				TableUtil.findColIndex(in.getColNames(), weightColName),
				featureIndices
			));

		DataSet <NaiveBayesModelData> naiveBayesModel = trainData
			.groupBy(new SelectLabel())
			.reduceGroup(new ReduceItem(isCate))
			.mapPartition(new GenerateModel(smoothing, featureColNames, labelType, isCate))
			.withBroadcastSet(stringIndexerModel.getDataSet(), "stringIndexerModel")
			.setParallelism(1);
		DataSet <Row> model = naiveBayesModel
			.flatMap(new FlatMapFunction <NaiveBayesModelData, Row>() {
				private static final long serialVersionUID = 4634517702171022715L;

				@Override
				public void flatMap(NaiveBayesModelData value, Collector <Row> out) throws Exception {
					new NaiveBayesModelDataConverter(labelType).save(value, out);
				}
			}).setParallelism(1);
		this.setOutput(model, new NaiveBayesModelDataConverter(labelType).getModelSchema());
		return this;
	}

	private static boolean[] generateCategoricalCols(String[] originCategoricalColNames,
													 String[] inputColNames, TypeInformation <?>[] inputTypes,
													 String labelColName, Params params) {
		List <String> categoricalCols = new ArrayList <>();
		int length = inputColNames.length;
		boolean[] isCate = new boolean[length];
		for (int i = 0; i < length; i++) {
			String colName = inputColNames[i];
			if (colName.equals(labelColName)) {
				continue;
			}
			TypeInformation <?> type = inputTypes[i];
			//if in original categorical cols, then check its type and then set it categorical.
			boolean typeSatisfy = checkCategorical(type);
			if (typeSatisfy ||
				((type.equals(Types.BIG_INT) || type.equals(Types.INT) || type.equals(Types.LONG))
					&& TableUtil.findColIndex(originCategoricalColNames, colName) != -1)) {
				//if it's categorical, if it's bigint and is not in the original list, then do not add it.
				categoricalCols.add(colName);
				isCate[i] = true;
			} else if (TableUtil.findColIndex(originCategoricalColNames, colName) != -1) {
				throw new AkIllegalOperatorParameterException("column \"" + colName + "\"'s type is " + type +
					", which is not categorical!");
			}
		}
		params.set(HasCategoricalCols.CATEGORICAL_COLS, categoricalCols.toArray(new String[0]));
		return isCate;
	}

	private static boolean checkCategorical(TypeInformation type) {
		if (type.equals(Types.STRING) || type.equals(Types.BOOLEAN)) {
			return true;
		} else if (type.equals(Types.DOUBLE) || type.equals(Types.FLOAT) ||
			type.equals(Types.BIG_INT) || type.equals(Types.LONG) || type.equals(Types.INT)) {
			return false;
		}
		throw new AkUnsupportedOperationException("don't support the type " + type);
	}

	@Override
	public NaiveBayesModelInfoBatchOp getModelInfoBatchOp() {
		return new NaiveBayesModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	private static class SelectLabel implements KeySelector <Tuple3 <Object, Double, Number[]>, String> {

		private static final long serialVersionUID = -8656348545181937312L;

		@Override
		public String getKey(Tuple3 <Object, Double, Number[]> t3) {
			return t3.f0.toString();
		}
	}

	/**
	 * Transform the data format.
	 */
	private static class Transform implements MapPartitionFunction <Row, Tuple3 <Object, Double, Number[]>> {
		private static final long serialVersionUID = 2035076744255855602L;
		private int labelColIndex;
		private int weightColIndex;
		private int[] featureColIndices;
		private int featureSize;

		Transform(int labelColIndex, int weightColIndex, int[] featureColIndices) {
			this.labelColIndex = labelColIndex;
			this.weightColIndex = weightColIndex;
			this.featureColIndices = featureColIndices;
			this.featureSize = featureColIndices.length;
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple3 <Object, Double, Number[]>> out)
			throws Exception {
			if (weightColIndex == -1) {
				for (Row value : values) {
					Object label = value.getField(labelColIndex);
					Number[] feature = new Number[this.featureSize];
					for (int i = 0; i < this.featureSize; i++) {
						feature[i] = (Number) value.getField(featureColIndices[i]);
					}
					out.collect(Tuple3.of(label, 1.0, feature));
				}
			} else {
				for (Row value : values) {
					Object label = value.getField(labelColIndex);
					Double weight = (Double) value.getField(weightColIndex);
					Number[] feature = new Number[this.featureSize];
					for (int i = 0; i < this.featureSize; i++) {
						feature[i] = (Number) value.getField(featureColIndices[i]);
					}
					out.collect(Tuple3.of(label, weight, feature));
				}
			}
		}
	}

	/**
	 * reduce and calculate the μ and σ in continuous features and reduce feature number in categorical features.
	 */
	private static class ReduceItem extends AbstractRichFunction
		implements GroupReduceFunction <Tuple3 <Object, Double, Number[]>,
		Tuple3 <Object, Double[], HashMap <Integer, Double>[]>> {
		private static final long serialVersionUID = 4271048412599201885L;
		boolean[] isCate;
		int featureSize;

		ReduceItem(boolean[] isCate) {
			this.isCate = isCate;
			this.featureSize = isCate.length;
		}

		@Override
		public void reduce(Iterable <Tuple3 <Object, Double, Number[]>> values,
						   Collector <Tuple3 <Object, Double[], HashMap <Integer, Double>[]>> out) throws Exception {
			Double[] weightSum = new Double[this.featureSize];
			Arrays.fill(weightSum, 0.);
			Object label = null;
			HashMap <Integer, Double>[] param = new HashMap[this.featureSize];
			for (int i = 0; i < featureSize; i++) {
				param[i] = new HashMap <Integer, Double>(2);
			}
			for (Tuple3 <Object, Double, Number[]> value : values) {
				label = value.f0;
				for (int i = 0; i < featureSize; i++) {
					if (value.f2[i] == null) {
						continue;
					}
					if (isCate[i]) {
						param[i].compute((Integer) value.f2[i], (k, v) -> v == null ? value.f1 : (double) v + value
							.f1);
					} else {
						HashMap <Integer, Double> map = param[i];
						if (!map.containsKey(0)) {
							map.put(0, value.f1 * (double) value.f2[i]);
							map.put(1, value.f1 * Math.pow((double) value.f2[i], 2));
						} else {
							map.put(0, map.get(0) + value.f1 * (double) value.f2[i]);
							map.put(1, map.get(1) + value.f1 * Math.pow((double) value.f2[i], 2));
						}
					}
					weightSum[i] += value.f1;
				}
			}
			for (int i = 0; i < featureSize; i++) {
				if (!isCate[i]) {
					HashMap <Integer, Double> map = param[i];
					map.put(0, map.get(0) / weightSum[i]);//μ
					double sigma = map.get(1) / weightSum[i] - Math.pow(map.get(0), 2);
					map.put(1, sigma);//σ^2
				}
			}
			out.collect(Tuple3.of(label, weightSum, param));
		}
	}

	private static class GenerateModel extends AbstractRichFunction
		implements MapPartitionFunction <Tuple3 <Object, Double[], HashMap <Integer, Double>[]>, NaiveBayesModelData> {
		private static final long serialVersionUID = -8733125133076037943L;
		private int featureSize;
		private double smoothing;
		private String[] featureColNames;
		private TypeInformation labelType;
		private boolean[] isCate;
		private List <Row> stringIndexerModel;

		GenerateModel(double smoothing, String[] featureColNames, TypeInformation labelType, boolean[] isCate) {
			this.smoothing = smoothing;
			this.labelType = labelType;
			this.featureColNames = featureColNames;
			this.isCate = isCate;
			this.featureSize = featureColNames.length;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.stringIndexerModel = getRuntimeContext()
				.getBroadcastVariable("stringIndexerModel");
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Object, Double[],
			HashMap <Integer, Double>[]>> values, Collector <NaiveBayesModelData> out) throws Exception {
			double[] numDocs = new double[featureSize];
			ArrayList <Tuple3 <Object, Double[], HashMap <Integer, Double>[]>> modelArray = new ArrayList <>();
			HashSet <Integer>[] categoryNumbers = new HashSet[featureSize];
			for (int i = 0; i < featureSize; i++) {
				categoryNumbers[i] = new HashSet <>();
			}
			for (Tuple3 <Object, Double[], HashMap <Integer, Double>[]> tup : values) {
				modelArray.add(tup);
				for (int i = 0; i < featureSize; i++) {
					numDocs[i] += tup.f1[i];
					categoryNumbers[i].addAll(tup.f2[i].keySet());
				}
			}

			int[] categoryNumber = new int[featureSize];
			double piLog = 0;
			int numLabels = modelArray.size();
			for (int i = 0; i < featureSize; i++) {
				categoryNumber[i] = categoryNumbers[i].size();
				piLog += numDocs[i];
			}
			piLog = Math.log(piLog + numLabels * smoothing);

			Number[][][] theta = new Number[numLabels][featureSize][];
			double[] piArray = new double[numLabels];
			double[] pi = new double[numLabels];
			Object[] labels = new Object[numLabels];

			//consider smoothing.
			for (int i = 0; i < numLabels; i++) {
				HashMap <Integer, Double>[] param = modelArray.get(i).f2;
				for (int j = 0; j < featureSize; j++) {
					int size = categoryNumber[j];
					Number[] squareData = new Number[size];
					if (isCate[j]) {
						double thetaLog = Math.log(modelArray.get(i).f1[j] + smoothing * categoryNumber[j]);
						for (int k = 0; k < size; k++) {
							double value = 0;
							if (param[j].containsKey(k)) {
								value = param[j].get(k);
							}
							squareData[k] = Math.log(value + smoothing) - thetaLog;
						}
					} else {
						for (int k = 0; k < size; k++) {
							squareData[k] = param[j].get(k);
						}
					}
					theta[i][j] = squareData;
				}

				labels[i] = modelArray.get(i).f0;
				double weightSum = 0;
				for (Double weight : modelArray.get(i).f1) {
					weightSum += weight;
				}
				pi[i] = weightSum;
				piArray[i] = Math.log(weightSum + smoothing) - piLog;
			}
			NaiveBayesModelData modelData = new NaiveBayesModelData();
			modelData.featureNames = featureColNames;
			modelData.isCate = isCate;
			modelData.label = labels;
			modelData.piArray = piArray;
			modelData.labelWeights = pi;
			modelData.theta = theta;
			modelData.stringIndexerModelSerialized = this.stringIndexerModel;
			modelData.generateWeightAndNumbers(modelArray);
			out.collect(modelData);
		}
	}
}
