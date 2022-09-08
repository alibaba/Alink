package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalModelException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.lazy.WithTrainInfo;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp.CreateMeta;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelTrainInfo;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.params.regression.AftRegTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_INFO),
	@PortSpec(value = PortType.DATA, desc = PortDesc.FEATURE_IMPORTANCE),
	@PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_WEIGHT)
})

@ParamSelectColumnSpec(name = "featureCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "censorCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@FeatureColsVectorColMutexRule

@NameCn("生存回归训练")
public class AftSurvivalRegTrainBatchOp extends BatchOperator <AftSurvivalRegTrainBatchOp>
	implements AftRegTrainParams <AftSurvivalRegTrainBatchOp>,
	WithTrainInfo <LinearModelTrainInfo, AftSurvivalRegTrainBatchOp>,
	WithModelInfoBatchOp <LinearRegressorModelInfo, AftSurvivalRegTrainBatchOp, AftSurvivalRegModelInfoBatchOp> {

	private static final long serialVersionUID = -7789949822832208166L;

	/**
	 * Constructor.
	 */
	public AftSurvivalRegTrainBatchOp() {
		this(null);
	}

	/**
	 * Constructor.
	 *
	 * @param params the params of the algorithm.
	 */
	public AftSurvivalRegTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public AftSurvivalRegTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator<?> in;
		BatchOperator<?> initModel = null;
		if (inputs.length == 1) {
			in = checkAndGetFirst(inputs);
		} else {
			in = inputs[0];
			initModel = inputs[1];
		}
		String[] featureColNames = this.getFeatureCols();
		TypeInformation<?> labelType = Types.DOUBLE;
		DataSet <Object> labelValues = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
			.fromElements(new Object());
		Params params = getParams();
		if (params.contains(HasFeatureCols.FEATURE_COLS) && params.contains(HasVectorCol.VECTOR_COL)) {
			throw new AkIllegalOperatorParameterException("featureCols and vectorCol cannot be set at the same time.");
		}
		params.set(LinearTrainParams.WEIGHT_COL, this.getCensorCol());
		DataSet <Tuple3 <Double, Object, Vector>> initData = BaseLinearModelTrainBatchOp
			.transform(in, params, true, true);
		DataSet <Tuple3 <DenseVector[], Object[], Integer[]>> utilInfo = BaseLinearModelTrainBatchOp
			.getUtilInfo(initData, true, true);

		DataSet <double[]> std = utilInfo.map(
			new MapFunction <Tuple3 <DenseVector[], Object[], Integer[]>, double[]>() {
				private static final long serialVersionUID = -7070926092286155032L;

				@Override
				public double[] map(Tuple3 <DenseVector[], Object[], Integer[]> value) {
					return value.f0[1].getData();
				}
			});

		DataSet <Integer> vectorSize = utilInfo.map(
			new MapFunction <Tuple3 <DenseVector[], Object[], Integer[]>, Integer>() {
				private static final long serialVersionUID = 5463028282798602155L;

				@Override
				public Integer map(Tuple3 <DenseVector[], Object[], Integer[]> value) {
					return value.f2[0];
				}
			});

		//censor/label, log(time), feature/std.
		DataSet <Tuple3 <Double, Double, Vector>> trainData = initData
			.mapPartition(new FormatLabeledVector())
			.withBroadcastSet(std, "std");

		DataSet <Params> meta = labelValues.map(new MapFunction <Object, Object[]>() {
			private static final long serialVersionUID = -1563051729748477019L;

			@Override
			public Object[] map(Object value) {
				return new Object[0];
			}
		})
			.mapPartition(new CreateMeta("AFTSurvivalRegTrainBatchOp", LinearModelType.AFT, getParams()))
			.setParallelism(1);

		DataSet <DenseVector> initModelDataSet = initModel == null ? null : initModel.getDataSet().reduceGroup(
			new GroupReduceFunction <Row, DenseVector>() {
				@Override
				public void reduce(Iterable <Row> values, Collector <DenseVector> out) {
					List <Row> modelRows = new ArrayList <>(0);
					for (Row row : values) {
						modelRows.add(row);
					}
					LinearModelData model = new LinearModelDataConverter().load(modelRows);
					try {
						assert (model.hasInterceptItem == params.get(HasWithIntercept.WITH_INTERCEPT));
					} catch (Exception e) {
						throw new AkIllegalModelException("initial model is not compatible with data and parameter setting.");
					}
					out.collect(model.coefVector);
				}
			});

		DataSet <Tuple2 <DenseVector, double[]>>
			coefVectorSet = BaseLinearModelTrainBatchOp.optimize(this.getParams(), vectorSize,
			trainData, initModelDataSet, LinearModelType.AFT, MLEnvironmentFactory.get(getMLEnvironmentId()));

		DataSet <Tuple2 <DenseVector, double[]>> coef = coefVectorSet
			.map(new FormatCoef())
			.withBroadcastSet(std, "std");

		DataSet <Row> modelRows = coef
			.mapPartition(
				new BaseLinearModelTrainBatchOp.BuildModelFromCoefs(labelType, featureColNames, false, false, null))
			.withBroadcastSet(meta, "meta")
			.setParallelism(1);

		this.setOutput(modelRows, new LinearModelDataConverter(labelType).getModelSchema());
		DataSet <Integer> featSize = utilInfo.map(
			new MapFunction <Tuple3 <DenseVector[], Object[], Integer[]>, Integer>() {
				private static final long serialVersionUID = 2773811388068064638L;

				@Override
				public Integer map(Tuple3 <DenseVector[], Object[], Integer[]> value) {
					return value.f2[0];
				}
			});
		this.setSideOutputTables(
			BaseLinearModelTrainBatchOp.getSideTablesOfCoefficient(modelRows, initData, featSize,
				params.get(LinearTrainParams.FEATURE_COLS),
				params.get(LinearTrainParams.WITH_INTERCEPT),
				getMLEnvironmentId()));
		return this;
	}

	@Override
	public AftSurvivalRegModelInfoBatchOp getModelInfoBatchOp() {
		return new AftSurvivalRegModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	/**
	 * Extract the label, weight and vector from input row, and standardize the train data.
	 */
	public static class FormatLabeledVector extends AbstractRichFunction
		implements MapPartitionFunction <Tuple3 <Double, Object, Vector>, Tuple3 <Double, Double, Vector>> {
		private static final long serialVersionUID = -1207608955281033320L;
		private double[] std;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.std = (double[]) (getRuntimeContext().getBroadcastVariable("std").get(0));
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Object, Vector>> rows,
								 Collector <Tuple3 <Double, Double, Vector>> out)
			throws Exception {
			for (Tuple3 <Double, Object, Vector> row : rows) {
				Double weight = row.getField(0);
				if (Math.abs(weight + 1) < 1e-4 || Math.abs(weight + 2) < 1e-4) {
					continue;
				}
				Vector tmpVector = row.getField(2);
				//this is the current time, and it cannot be negative.
				double val = (double) row.f1;
				if (val <= 0) {
					throw new AkIllegalArgumentException("Survival Time must be greater than 0!");
				}
				//judge whether the weight is legal or not.
				if (!weight.equals(0.0) && !weight.equals(1.0)) {
					throw new AkIllegalArgumentException("Censor must be 1.0 or 0.0!");
				}
				Vector aVector;
				if (tmpVector instanceof SparseVector) {
					aVector = svStd(tmpVector, std);
				} else {
					aVector = dvStd(tmpVector, std);
				}
				out.collect(new Tuple3 <>(weight, Math.log(val), aVector));
			}
		}
	}

	private static Vector svStd(Vector tmpVector, double[] std) {
		SparseVector sv = (SparseVector) tmpVector;
		int[] index = sv.getIndices();
		double[] values = sv.getValues();
		int size = index.length;
		for (int i = 0; i < size; i++) {
			values[i] /= std[index[i]];
		}
		return sv;
	}

	private static Vector dvStd(Vector tmpVector, double[] std) {
		DenseVector dv = (DenseVector) tmpVector;
		double[] values = dv.getData();
		int size = values.length;
		for (int i = 0; i < size; i++) {
			values[i] /= std[i];
		}
		return dv;
	}

	/**
	 * Regulate the coefficients from standardized data.
	 */
	public static class FormatCoef extends AbstractRichFunction
		implements MapFunction <Tuple2 <DenseVector, double[]>, Tuple2 <DenseVector, double[]>> {
		private static final long serialVersionUID = -5330623238434515619L;
		private double[] std;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.std = (double[]) (getRuntimeContext().getBroadcastVariable("std").get(0));
		}

		@Override
		public Tuple2 <DenseVector, double[]> map(Tuple2 <DenseVector, double[]> aVector) throws Exception {
			DenseVector vec = aVector.f0.clone();
			double[] vecData = vec.getData();
			double[] aVectorData = aVector.f0.getData();
			int size = aVectorData.length - 1;
			for (int i = 0; i < size; ++i) {
				if (std[i] > 0) {
					vecData[i] = aVectorData[i] / std[i];
				} else {
					vecData[i] = 0.0;
				}
			}
			return Tuple2.of(vec, aVector.f1);
		}
	}

	@Override
	public LinearModelTrainInfo createTrainInfo(List <Row> rows) {
		return new LinearModelTrainInfo(rows);
	}

	@Override
	public BatchOperator <?> getSideOutputTrainInfo() {
		return getSideOutput(0);
	}

}
