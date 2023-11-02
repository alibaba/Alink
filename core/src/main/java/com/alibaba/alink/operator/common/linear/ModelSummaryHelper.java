package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.BaseStepWiseSelectorBatchOp;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.ClassificationSelectorResult;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.RegressionSelectorResult;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorResult;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.params.finance.HasConstrainedLinearModelType;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper.transformToVector;

public class ModelSummaryHelper {

	public static DataSet <SelectorResult> calModelSummary(BatchOperator data,
														   HasConstrainedLinearModelType.LinearModelType
															   linearModelType,
														   DataSet <LinearModelData> modelData,
														   String vectorCol,
														   String[] selectedCols,
														   String labelCol) {
		if (HasConstrainedLinearModelType.LinearModelType.LR == linearModelType) {
			return calBinarySummary(data, modelData, vectorCol, selectedCols, labelCol)
				.map(new MapFunction <LogistRegressionSummary, SelectorResult>() {
					private static final long serialVersionUID = -3321750369444781812L;

					@Override
					public SelectorResult map(LogistRegressionSummary summary) throws Exception {
						ClassificationSelectorResult result = new ClassificationSelectorResult();
						result.modelSummary = summary;
						result.selectedCols = selectedCols;
						return result;
					}
				});
		} else {
			return calRegSummary(data, modelData, vectorCol, selectedCols, labelCol)
				.map(new MapFunction <LinearRegressionSummary, SelectorResult>() {
					private static final long serialVersionUID = 2198755005878386785L;

					@Override
					public SelectorResult map(LinearRegressionSummary summary) throws Exception {
						RegressionSelectorResult result = new RegressionSelectorResult();
						result.modelSummary = summary;
						result.selectedCols = selectedCols;
						return result;
					}
				});
		}
	}

	public static DataSet <LinearRegressionSummary> calRegSummary(BatchOperator data,
																  DataSet <LinearModelData> modelData,
																  String vectorCol,
																  String[] selectedCols,
																  String labelCol) {
		DataSet <BaseVectorSummarizer> summarizer = null;
		if (null != vectorCol && !vectorCol.isEmpty()) {
			int selectedColIdxNew = TableUtil.findColIndexWithAssertAndHint(data.getColNames(), vectorCol);
			int labelIdxNew = TableUtil.findColIndexWithAssertAndHint(data.getColNames(), labelCol);
			summarizer = StatisticsHelper.summarizer(data.getDataSet()
				.map(new BaseStepWiseSelectorBatchOp.ToVectorWithReservedCols(selectedColIdxNew, labelIdxNew)), true);
		}

		if (null != selectedCols && selectedCols.length != 0) {
			String[] statCols = new String[selectedCols.length + 1];
			statCols[0] = labelCol;
			System.arraycopy(selectedCols, 0, statCols, 1, selectedCols.length);
			summarizer = StatisticsHelper.summarizer(transformToVector(data, statCols, null), true);
		}

		if (null == summarizer) {
			throw new InvalidParameterException("select col and select cols must be set one");
		}

		return summarizer
			.flatMap(new CalRegSummary())
			.withBroadcastSet(modelData, "linearModelData");

	}

	public static DataSet <LogistRegressionSummary> calBinarySummary(BatchOperator data,
																	 DataSet <LinearModelData> modelData,
																	 String vectorCol,
																	 String[] featureCols,
																	 String labelCol) {

		int[] featureIndices = null;
		int labelIdx = TableUtil.findColIndexWithAssertAndHint(data.getColNames(), labelCol);
		if (featureCols != null) {
			featureIndices = new int[featureCols.length];
			for (int i = 0; i < featureCols.length; ++i) {
				int idx = TableUtil.findColIndexWithAssertAndHint(data.getColNames(), featureCols[i]);
				featureIndices[i] = idx;
				TypeInformation type = data.getSchema().getFieldTypes()[idx];
				Preconditions.checkState(TableUtil.isSupportedNumericType(type),
					"linear algorithm only support numerical data type. type is : " + type);
			}
		}
		int weightIdx = -1;
		int vecIdx = vectorCol != null ? TableUtil.findColIndexWithAssertAndHint(data.getColNames(), vectorCol) : -1;

		//transform data
		DataSet <Tuple3 <Double, Double, Vector>> tuple3Data = data.getDataSet()
			.map(new TransformLrLabel(vecIdx, featureIndices, labelIdx, weightIdx))
			.withBroadcastSet(modelData, "linearModelData")
			.name("TransferLrData");

		//summarizer
		DataSet <BaseVectorSummarizer> summarizer = StatisticsHelper.summarizer(
			tuple3Data.map(new MapFunction <Tuple3 <Double, Double, Vector>, Vector>() {
				private static final long serialVersionUID = -1205844850032698897L;

				@Override
				public Vector map(Tuple3 <Double, Double, Vector> tuple3) throws Exception {
					tuple3.f2.set(0, tuple3.f0);
					return tuple3.f2;
				}
			}), false);

		//stat
		return tuple3Data.mapPartition(new CalcGradientAndHessian())
			.withBroadcastSet(modelData, "linearModelData")
			.reduce(new ReduceFunction <Tuple4 <DenseVector, DenseVector, DenseMatrix, Double>>() {
				private static final long serialVersionUID = -9187304403661961376L;

				@Override
				public Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> reduce(
					Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> left,
					Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> right) throws Exception {
					return Tuple4.of(left.f0,
						left.f1.plus(right.f1),
						left.f2.plus(right.f2),
						left.f3 + right.f3
					);
				}
			}).name("combine gradient and hessian")
			.mapPartition(new CalcLrSummary())
			.withBroadcastSet(summarizer, "Summarizer");
	}

	/**
	 * return: data, labels
	 */
	public static Tuple2 <BatchOperator, DataSet <Object>> transformLrLabel(BatchOperator data,
																			String labelCol,
																			String positiveLabel,
																			Long sessionId) {
		int labelColIdx = TableUtil.findColIndexWithAssertAndHint(data.getColNames(), labelCol);

		DataSet <Object> lables = getLabelInfo(data, labelCol);
		DataSet <Row> transformedData = data.getDataSet()
			.map(new TransformLrLabelWithLabel(labelColIdx, positiveLabel))
			.withBroadcastSet(lables, "labels");

		TypeInformation[] outTypes = data.getColTypes();
		outTypes[labelColIdx] = Types.DOUBLE;

		return Tuple2.of(new TableSourceBatchOp(
				DataSetConversionUtil.toTable(sessionId, transformedData, data.getColNames(), outTypes))
			, lables);
	}

	public static class TransformLrLabel extends RichMapFunction <Row, Tuple3 <Double, Double, Vector>> {
		private static final long serialVersionUID = -1178726287840080632L;
		private LinearModelData modelData;
		private String positiveLableValueString;
		private int vecIdx;
		private int[] featureIndices;
		private int labelIdx;
		private int weightIdx;

		public TransformLrLabel(int vecIdx, int[] featureIndices, int labelIdx, int weightIdx) {
			this.vecIdx = vecIdx;
			this.featureIndices = featureIndices;
			this.labelIdx = labelIdx;
			this.weightIdx = weightIdx;
		}

		public void open(Configuration conf) {
			this.modelData = (LinearModelData) this.getRuntimeContext().
				getBroadcastVariable("linearModelData")
				.get(0);
			this.positiveLableValueString = this.modelData.labelValues[0].toString();
		}

		@Override
		public Tuple3 <Double, Double, Vector> map(Row row) throws Exception {
			return transferLabel(row, featureIndices, vecIdx, labelIdx, weightIdx, positiveLableValueString);
		}
	}

	public static class TransformLrLabelWithLabel extends RichMapFunction <Row, Row> {
		private static final long serialVersionUID = 9009298353079015437L;
		private String positiveLableValueString = null;
		private int labelIdx;

		public TransformLrLabelWithLabel(int labelIdx, String positiveLabel) {
			this.labelIdx = labelIdx;
			this.positiveLableValueString = positiveLabel;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Object> labelRows = getRuntimeContext().getBroadcastVariable("labels");
			this.positiveLableValueString = orderLabels(labelRows, positiveLableValueString)[0].toString();
		}

		@Override
		public Row map(Row row) throws Exception {
			Double val = FeatureLabelUtil.getLabelValue(row, false,
				labelIdx, positiveLableValueString);
			row.setField(labelIdx, val);
			return row;
		}
	}

	public static Object[] orderLabels(Iterable <Object> unorderedLabelRows, String positiveLabel) {
		List <Object> tmpArr = new ArrayList <>();
		for (Object row : unorderedLabelRows) {
			tmpArr.add(row);
		}
		Object[] labels = tmpArr.toArray(new Object[0]);
		Preconditions.checkState((labels.length == 2), "labels count should be 2 in 2 classification algo.");
		String str0 = labels[0].toString();
		String str1 = labels[1].toString();

		String positiveLabelValueString = positiveLabel;
		if (positiveLabelValueString == null) {
			positiveLabelValueString = (str1.compareTo(str0) > 0) ? str1 : str0;
		}

		if (labels[1].toString().equals(positiveLabelValueString)) {
			Object t = labels[0];
			labels[0] = labels[1];
			labels[1] = t;
		}
		return labels;
	}

	public static class CalcGradientAndHessian extends RichMapPartitionFunction <Tuple3 <Double, Double, Vector>,
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double>> {
		private static final long serialVersionUID = 2861532185191763285L;
		private LinearModelData modelData;

		public void open(Configuration conf) {
			this.modelData = (LinearModelData) this.getRuntimeContext().
				getBroadcastVariable("linearModelData")
				.get(0);
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Double, Vector>> data,
								 Collector <Tuple4 <DenseVector, DenseVector, DenseMatrix, Double>> collector)
			throws Exception {
			final OptimObjFunc objFunc = OptimObjFunc.getObjFunction(LinearModelType.LR,
				new Params());
			int n = modelData.coefVector.size();
			DenseVector coef = modelData.coefVector;
			DenseMatrix hessian = new DenseMatrix(n, n);
			DenseVector gradient = new DenseVector(n);

			Tuple2 <Double, Double> tuple2 = objFunc.calcHessianGradientLoss(data, coef, hessian, gradient);
			//LogistRegressionSummary summary = LocalLinearModel.calcLrSummary(Tuple4.of(coef, gradient, hessian,
			// tuple2.f1), BaseVectorSummarizer);
			collector.collect(Tuple4.of(coef, gradient, hessian, tuple2.f1));
		}
	}

	public static class CalcLrSummary extends RichMapPartitionFunction <
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double>,
		LogistRegressionSummary> {
		private static final long serialVersionUID = 1799381476070262842L;
		private BaseVectorSummarizer srt;

		public void open(Configuration conf) {
			this.srt = (BaseVectorSummarizer) this.getRuntimeContext().
				getBroadcastVariable("Summarizer")
				.get(0);
		}

		@Override
		public void mapPartition(Iterable <Tuple4 <DenseVector, DenseVector, DenseMatrix, Double>> model,
								 Collector <LogistRegressionSummary> collector) throws Exception {
			Iterator <Tuple4 <DenseVector, DenseVector, DenseMatrix, Double>> iter = model.iterator();
			if (iter.hasNext()) {
				collector.collect(LocalLinearModel.calcLrSummary(iter.next(), srt));
			}
		}
	}

	public static class CalRegSummary extends RichFlatMapFunction <BaseVectorSummarizer, LinearRegressionSummary> {
		private static final long serialVersionUID = 1372774780273725623L;
		private LinearModelData modelData;

		public void open(Configuration conf) {
			this.modelData = (LinearModelData) this.getRuntimeContext().
				getBroadcastVariable("linearModelData")
				.get(0);
		}

		@Override
		public void flatMap(BaseVectorSummarizer summarizer, Collector <LinearRegressionSummary> collector)
			throws Exception {
			DenseVector beta = modelData.coefVector;
			int vectorSize = summarizer.toSummary().vectorSize();
			int[] statIndices = new int[vectorSize - 1];
			for (int i = 0; i < statIndices.length; i++) {
				statIndices[i] = i + 1;
			}
			collector.collect(LocalLinearModel.calcLinearRegressionSummary(beta, summarizer, 0, statIndices));
		}
	}

	public static Boolean isLinearRegression(String linearModelType) {
		linearModelType = linearModelType.trim().toUpperCase();
		if (!linearModelType.equals("LINEARREG") && !linearModelType.equals("LR") && !linearModelType.equals("DIVERGENCE") ) {
			throw new RuntimeException("model type not support. " + linearModelType);
		}
		return "LINEARREG".equals(linearModelType);
	}

	public static Boolean isLogisticRegression(String linearModelType) {
		linearModelType = linearModelType.trim().toUpperCase();
		if (!linearModelType.equals("LINEARREG") && !linearModelType.equals("LR") && !linearModelType.equals("DIVERGENCE") ) {
			throw new RuntimeException("model type not support. " + linearModelType);
		}
		return "LR".equals(linearModelType);
	}

	public static DataSet <Object> getLabelInfo(BatchOperator in,
												String labelCol) {
		return in.select(new String[] {labelCol}).distinct().getDataSet().map(
			new MapFunction <Row, Object>() {
				private static final long serialVersionUID = 2044498497762182626L;

				@Override
				public Object map(Row row) {
					return row.getField(0);
				}
			});
	}

	private static Tuple3 <Double, Double, Vector> transferLabel(Row row,
																 int[] featureIndices, int vecIdx, int labelIdx,
																 int weightIdx,
																 String positiveLableValueString) throws Exception {
		Double weight = weightIdx != -1 ? ((Number) row.getField(weightIdx)).doubleValue() : 1.0;
		Double val = FeatureLabelUtil.getLabelValue(row, false,
			labelIdx, positiveLableValueString);
		Tuple3 <Double, Double, Vector> tuple3;
		if (featureIndices != null) {
			DenseVector vec = new DenseVector(featureIndices.length);
			for (int i = 0; i < featureIndices.length; ++i) {
				vec.set(i, ((Number) row.getField(featureIndices[i])).doubleValue());
			}
			tuple3 = Tuple3.of(weight, val, vec);
		} else {
			Vector vec = VectorUtil.getVector(row.getField(vecIdx));
			Preconditions.checkState((vec != null),
				"vector for linear model train is null, please check your input data.");

			tuple3 = Tuple3.of(weight, val, vec);
		}

		return Tuple3.of(tuple3.f0, tuple3.f1, tuple3.f2.prefix(1.0));
	}

}
