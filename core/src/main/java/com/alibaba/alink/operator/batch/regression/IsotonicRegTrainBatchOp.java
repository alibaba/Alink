package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamCond;
import com.alibaba.alink.common.annotation.ParamCond.CondType;
import com.alibaba.alink.common.annotation.ParamMutexRule;
import com.alibaba.alink.common.annotation.ParamMutexRule.ActionType;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.regression.IsotonicRegressionConverter;
import com.alibaba.alink.operator.common.regression.IsotonicRegressionModelData;
import com.alibaba.alink.operator.common.regression.isotonicReg.LinkedData;
import com.alibaba.alink.params.regression.IsotonicRegTrainParams;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Isotonic regression.
 * Implement using parallelized pool adjacent violators algorithm.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Isotonic_regression">Isotonic regression
 * (Wikipedia)</a>
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(PortType.MODEL)})
@ParamSelectColumnSpec(name = "labelCol",allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "weightCol",allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "featureCol",allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",allowedTypeCollections= TypeCollections.VECTOR_TYPES)
@ParamMutexRule(
	name = "vectorCol", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "featureCol",
		type = CondType.WHEN_NOT_NULL
	)
)
@ParamMutexRule(
	name = "featureCol", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "vectorCol",
		type = CondType.WHEN_NOT_NULL
	)
)

@NameCn("保序回归训练")
public final class IsotonicRegTrainBatchOp extends BatchOperator <IsotonicRegTrainBatchOp>
	implements IsotonicRegTrainParams <IsotonicRegTrainBatchOp> {

	private static final long serialVersionUID = -1681187909098085588L;

	/**
	 * Constructor.
	 */
	public IsotonicRegTrainBatchOp() {
		super(new Params());
	}

	/**
	 * Constructor.
	 *
	 * @param params the params of the algorithm.
	 */
	public IsotonicRegTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public IsotonicRegTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = this.getLabelCol();
		String featureColName = this.getFeatureCol();
		String weightColName = this.getWeightCol();
		String vectorColName = this.getVectorCol();
		boolean isotonic = this.getIsotonic();
		int index = this.getFeatureIndex();
		// initialize the selectedColNames.
		String[] selectedColNames;
		if (null == vectorColName && null != featureColName) {
			if (weightColName == null) {
				selectedColNames = new String[] {labelColName, featureColName};
			} else {
				selectedColNames = new String[] {labelColName, featureColName, weightColName};
			}
		} else if (null == featureColName && null != vectorColName) {
			if (weightColName == null) {
				selectedColNames = new String[] {labelColName, vectorColName};
			} else {
				selectedColNames = new String[] {labelColName, vectorColName, weightColName};
			}
		} else if (null != featureColName) {
			throw new AkIllegalOperatorParameterException("featureCols and vectorCol cannot be set at the same time.");
		} else {
			throw new AkIllegalOperatorParameterException("Either featureColName or vectorColName is required!");
		}

		//initialize the input data, the three dimensions are label, feature, weight.
		DataSet <Tuple3 <Double, Double, Double>> dataSet = in.select(selectedColNames)
			.getDataSet()
			.map(new MapFunction <Row, Tuple3 <Double, Double, Double>>() {
					 private static final long serialVersionUID = 9034460902571223806L;

					 @Override
					 public Tuple3 <Double, Double, Double> map(Row row) {
						 double label = ((Number) row.getField(0)).doubleValue();
						 label = isotonic ? label : -label;
						 double feature = null == vectorColName ? ((Number) row.getField(1)).doubleValue()
							 : VectorUtil.getVector(row.getField(1)).get(index);
						 double weight = null == weightColName ? 1.0 : ((Number) row.getField(2)).doubleValue();
						 if (weight < 0) {
							 throw new AkIllegalDataException("Weights must be non-negative!");
						 }
						 return Tuple3.of(label, feature, weight);
					 }
				 }
			);

		DataSet <byte[]> model = dataSet
			.filter(new FilterFunction <Tuple3 <Double, Double, Double>>() {
				private static final long serialVersionUID = 234777408534250527L;

				@Override
				public boolean filter(Tuple3 <Double, Double, Double> value) {
					return value.f2 > 0;
				}
			})
			.rebalance()
			.partitionByRange(1)
			.mapPartition(new PoolAdjacentViolators());
		DataSet <Row> res = model
			.mapPartition(new BuildModel(isotonic, featureColName, vectorColName, index))
			.setParallelism(1);
		this.setOutput(res, new IsotonicRegressionConverter().getModelSchema());
		return this;
	}

	/**
	 * Retrieve the data of boundaries and values from all the parallel partitions,
	 * summary and then generate the IsotonicRegressionModel.
	 */
	public static class BuildModel implements MapPartitionFunction <byte[], Row> {
		private static final long serialVersionUID = -8409030153684329440L;
		private final boolean isotonic;
		private final String featureColName;
		private final String vectorColName;
		private final int index;

		BuildModel(boolean isotonic, String featureColName, String vectorColName, int index) {
			this.isotonic = isotonic;
			this.featureColName = featureColName;
			this.vectorColName = vectorColName;
			this.index = index;
		}

		@Override
		public void mapPartition(Iterable <byte[]> distributedModelData, Collector <Row> collector) {
			byte[] concatData = summarizeModelData(distributedModelData);
			byte[] res = updateLinkedData(new LinkedData(concatData));
			LinkedData mergedData = new LinkedData(res);
			List <Double> boundaries = new ArrayList <>();
			List <Double> values = new ArrayList <>();
			while (mergedData.hasNext()) {
				Tuple4 <Float, Double, Double, Float> currentData = mergedData.getData();
				float currentFloat1 = currentData.f0;
				double currentDouble1 = currentData.f1;
				double currentDouble2 = currentData.f2;
				float currentFloat2 = currentData.f3;
				double value = isotonic ? currentFloat1 / currentFloat2 :
					-currentFloat1 / currentFloat2;
				if (currentDouble1 == currentDouble2) {
					boundaries.add(currentDouble1);
					values.add(value);
				} else {
					boundaries.add(currentDouble1);
					values.add(value);
					boundaries.add(currentDouble2);
					values.add(value);
				}
				mergedData.advance();
			}

			IsotonicRegressionModelData modelData = new IsotonicRegressionModelData();
			modelData.boundaries = boundaries.toArray(new Double[0]);
			modelData.values = values.toArray(new Double[0]);
			modelData.meta.set(IsotonicRegTrainParams.FEATURE_COL, featureColName);
			modelData.meta.set(IsotonicRegTrainParams.VECTOR_COL, vectorColName);
			modelData.meta.set(IsotonicRegTrainParams.FEATURE_INDEX, index);
			new IsotonicRegressionConverter().save(modelData, collector);
		}
	}

	/**
	 * Implement parallelized pool adjacent violators algorithm.
	 */
	public static class PoolAdjacentViolators
		implements MapPartitionFunction <Tuple3 <Double, Double, Double>, byte[]> {
		private static final long serialVersionUID = -212769047923494155L;

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Double, Double>> tuple,
								 Collector <byte[]> collector) {
			if (null == tuple) {
				return;
			}
			byte[] list = updateLinkedData(initLinkedData(tuple));

			if (list.length > 0) {
				collector.collect(list);
			}
		}
	}

	/**
	 * Initialize the linked data for update.
	 */
	private static LinkedData initLinkedData(Iterable <Tuple3 <Double, Double, Double>> tuple) {
		ArrayList <Tuple3 <Double, Double, Double>> listData = Lists.newArrayList(tuple);
		listData.sort(Comparator.comparing(o -> o.f1));
		return new LinkedData(listData);
	}

	/**
	 * Update the linked data, generate boundary set of the input data.
	 */
	private static byte[] updateLinkedData(LinkedData linkedData) {
		if (null == linkedData || null == linkedData.getByteArray() || linkedData.getByteArray().length == 0) {
			return new byte[0];
		}

		Tuple4 <Float, Double, Double, Float> preBlock = linkedData.getData();
		float preLabel = preBlock.f0;
		double preStart = preBlock.f1;
		float preW = preBlock.f3;
		while (linkedData.hasNext()) {
			linkedData.advance();
			Tuple4 <Float, Double, Double, Float> currentBlock = linkedData.getData();

			float curLabel = currentBlock.f0;
			double curStart = currentBlock.f1;
			double curEnd = currentBlock.f2;
			float curW = currentBlock.f3;
			double preWeight = preLabel / preW;
			double curWeight = curLabel / curW;
			if (preWeight >= curWeight) {
				linkedData.removeCurrentAndRetreat();
				linkedData.putData(curLabel + preLabel, preStart, curEnd, preW + curW);
				if (linkedData.hasPrevious()) {
					linkedData.retreat();
				}
				preBlock = linkedData.getData();
				preLabel = preBlock.f0;
				preStart = preBlock.f1;
				preW = preBlock.f3;
			} else {
				preLabel = curLabel;
				preStart = curStart;
				preW = curW;
			}
		}
		int nonEmptyArrayLength = linkedData.compact();
		return Arrays.copyOfRange(
			linkedData.getByteArray(), 0, nonEmptyArrayLength * 24);
	}

	/**
	 * summary the distributed model data and then generate the model data.
	 */
	private static byte[] summarizeModelData(Iterable <byte[]> distributedModelData) {
		List <ByteBuffer> list = new ArrayList <>();
		int totalLength = 0;
		for (byte[] bytes : distributedModelData) {
			totalLength += bytes.length;
			list.add(ByteBuffer.wrap(bytes));
		}
		list.sort(Comparator.comparingDouble(o -> o.getDouble(4)));
		ByteBuffer allData = ByteBuffer.allocate(totalLength);
		for (ByteBuffer link : list) {
			allData.put(link);
		}
		return allData.array();
	}

}

