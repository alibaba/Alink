package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.OcsvmModelData;
import com.alibaba.alink.operator.common.outlier.OcsvmModelData.SvmModelData;
import com.alibaba.alink.operator.common.outlier.OcsvmModelDataConverter;
import com.alibaba.alink.params.outlier.OcsvmModelTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.alibaba.alink.operator.common.outlier.OcsvmKernel.svmTrain;

/**
 * One class SVM algorithm. we use the libsvm package (https://www.csie.ntu.edu.tw/~cjlin/libsvm/) to solve one class
 * svm problem with one thread, and using bagging algo to improve the scale of dataset.
 *
 * @author weibo zhao
 */
@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@NameCn("One Class SVM异常检测模型训练")
@NameEn("Ocsvm outlier model train")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.outlier.OcsvmModelOutlier")
public final class OcsvmModelOutlierTrainBatchOp extends BatchOperator <OcsvmModelOutlierTrainBatchOp>
	implements OcsvmModelTrainParams <OcsvmModelOutlierTrainBatchOp> {

	private static final long serialVersionUID = 6727016080849088600L;

	public OcsvmModelOutlierTrainBatchOp() {
		super(new Params());
	}

	public OcsvmModelOutlierTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public OcsvmModelOutlierTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String[] featureColNames = getFeatureCols();
		String tensorColName = getVectorCol();
		if ("".equals(tensorColName)) {
			tensorColName = null;
		}
		if (featureColNames != null && featureColNames.length == 0) {
			featureColNames = null;
		}
		final double nu = getNu();
		DataSet <Row> data;
		if (tensorColName != null || featureColNames == null) {
			// select feature data
			data = in.select(tensorColName).getDataSet();
		} else {
			int[] featureIndices = new int[featureColNames.length];
			for (int i = 0; i < featureColNames.length; ++i) {
				featureIndices[i] = TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
					featureColNames[i]);
			}
			data = in.getDataSet().map(new SelectFeat(featureIndices));
		}

		DataSet <Integer> bNumber =
			 data.mapPartition(new CalculateNum(featureColNames, tensorColName))
				.reduce(new ReduceFunction <Tuple3 <Integer, Integer, Integer>>() {
					private static final long serialVersionUID = 2307240714136503892L;

					@Override
					public Tuple3 <Integer, Integer, Integer> reduce(Tuple3 <Integer, Integer, Integer> t1,
																	 Tuple3 <Integer, Integer, Integer> t2) {
						return Tuple3.of(t1.f0 + t2.f0, Math.max(t1.f1, t2.f1), t2.f2);
					}
				}).map(new RichMapFunction <Tuple3 <Integer, Integer, Integer>, Integer>() {
					@Override
					public Integer map(Tuple3 <Integer, Integer, Integer> num) {
						if (num.f1 < 10) {
							return Math.max(num.f2, (int) Math.ceil(num.f0 * num.f1 * nu / 20000.0));
						} else if (num.f1 < 100 && num.f1 > 10) { // if feature length < 100
							return Math.max(num.f2, (int) Math.ceil(num.f0 * num.f1 * nu / 100000.0));
						} else {
							return Math.max(num.f2, (int) Math.ceil(num.f0 * nu / 1000.0));
						}
					}
				});

		// append the key to dataOp for groupby
		DataSet <Tuple2 <Integer, Row>> trainData
			= data.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Row>>() {
			private int bNumber;
			private final Random rand = new Random();

			@Override
			public void open(Configuration parameters) {
				bNumber = (Integer) getRuntimeContext().getBroadcastVariable("bNumber").get(0);
			}

			@Override
			public void mapPartition(Iterable <Row> rows, Collector <Tuple2 <Integer, Row>> out) {
				for (Row row : rows) {
					Integer key = this.rand.nextInt(this.bNumber);
					out.collect(Tuple2.of(key, row));
				}
			}
		}).withBroadcastSet(bNumber, "bNumber");

		// train
		DataSet <Tuple2<Double, SvmModelData>> models = trainData.groupBy(0)
			.reduceGroup(new TrainSvm(getParams()));

		// transform the models
		DataSet <Row> model = models
			.mapPartition(new Transform(getParams()))
			.withBroadcastSet(bNumber, "bNumber")
			.setParallelism(1);

		this.setOutput(model, new OcsvmModelDataConverter().getModelSchema());

		return this;
	}

	public static class SelectFeat implements MapFunction <Row, Row> {
		private static final long serialVersionUID = 331016784088329722L;
		private final int[] featureIndices;

		public SelectFeat(int[] featureIndices) {
			this.featureIndices = featureIndices;
		}

		@Override
		public Row map(Row value) throws Exception {
			Row ret = new Row(featureIndices.length);
			for (int i = 0; i < featureIndices.length; ++i) {
				ret.setField(i, ((Number) value.getField(featureIndices[i])).doubleValue());
			}
			return ret;
		}
	}

	public static class Transform extends AbstractRichFunction
		implements MapPartitionFunction <Tuple2<Double, SvmModelData>, Row> {
		private static final long serialVersionUID = -8875298030671722207L;
		private final String[] featureColNames;
		private int baggingNumber;
		private final KernelType kernelType;
		private final int degree;
		private double gamma;
		private final double coef0;
		private final String vectorCol;

		public Transform(Params params) {
			this.featureColNames = params.get(OcsvmModelTrainParams.FEATURE_COLS);
			this.kernelType = params.get(OcsvmModelTrainParams.KERNEL_TYPE);
			this.degree = params.get(OcsvmModelTrainParams.DEGREE);
			this.coef0 = params.get(OcsvmModelTrainParams.COEF0);
			this.vectorCol = params.get(OcsvmModelTrainParams.VECTOR_COL);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			baggingNumber = (Integer) getRuntimeContext()
				.getBroadcastVariable("bNumber").get(0);
		}

		@Override
		public void mapPartition(Iterable <Tuple2<Double, SvmModelData>> iterable, Collector <Row> collector) throws Exception {
			List <SvmModelData> models = new ArrayList <>();
			int size = 0;
			for (Tuple2<Double, SvmModelData> model : iterable) {
				models.add(model.f1);
				gamma = model.f0;
				size++;
			}

			SvmModelData[] modelArray = new SvmModelData[size];
			for (int i = 0; i < size; ++i) {
				modelArray[i] = models.get(i);
			}
			if (modelArray.length != 0) {
				OcsvmModelData ocsvmModelData = new OcsvmModelData();
				ocsvmModelData.models = modelArray;
				ocsvmModelData.featureColNames = featureColNames;
				ocsvmModelData.baggingNumber = baggingNumber;
				ocsvmModelData.kernelType = kernelType;
				ocsvmModelData.coef0 = coef0;
				ocsvmModelData.degree = degree;
				ocsvmModelData.gamma = gamma;
				ocsvmModelData.vectorCol = vectorCol;
				new OcsvmModelDataConverter().save(ocsvmModelData, collector);

			}
		}
	}

	public static class TrainSvm implements GroupReduceFunction <Tuple2 <Integer, Row>, Tuple2<Double, SvmModelData>> {
		private static final long serialVersionUID = -2783415250850319839L;
		private final Params param;
		private final String tensorColName;

		TrainSvm(Params param) {
			this.param = param;
			this.tensorColName = param.get(OcsvmModelTrainParams.VECTOR_COL);
		}

		@Override
		public void reduce(Iterable <Tuple2 <Integer, Row>> its,
						   Collector <Tuple2<Double, SvmModelData>> collector) throws Exception {
			List <Double> vy = new ArrayList <>();
			List <Vector> vectors = new ArrayList <>();
			int maxIndex = 0;
			int numRows = 0;
			if (this.tensorColName != null) {
				for (Tuple2 <Integer, Row> it : its) {
					numRows++;
					vy.add(0.0);
					Object obj = it.f1.getField(0);
					vectors.add(VectorUtil.getVector(obj));
				}
			} else {
				for (Tuple2 <Integer, Row> it : its) {
					numRows++;
					vy.add(0.0);
					int size = it.f1.getArity();
					maxIndex = size;
					Vector vec = new DenseVector(size);
					for (int i = 0; i < size; ++i) {
						vec.set(i, ((Number) it.f1.getField(i)).doubleValue());
					}
					vectors.add(vec);
				}
			}
			if (numRows > 0) {
				Vector[] sample = new Vector[vy.size()];
				for (int i = 0; i < vy.size(); i++) {
					sample[i] = vectors.get(i);
				}

				if (Math.abs(param.get(OcsvmModelTrainParams.GAMMA)) < 1.0e-18 && maxIndex > 0) {
					param.set(OcsvmModelTrainParams.GAMMA, 1.0 / maxIndex);
				}
				SvmModelData model = svmTrain(sample, param);
				collector.collect(Tuple2.of(1.0/maxIndex, model));
			}
		}
	}

	public static class CalculateNum extends RichMapPartitionFunction <Row, Tuple3 <Integer, Integer, Integer>> {
		private static final long serialVersionUID = -679835005763383100L;
		private final String[] featureColnames;
		private final String tensorColName;

		public CalculateNum(String[] featureColnames, String tensorColName) {
			this.featureColnames = featureColnames;
			this.tensorColName = tensorColName;
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Tuple3 <Integer, Integer, Integer>> out)
			throws Exception {
			int parallel = getRuntimeContext().getNumberOfParallelSubtasks();
			int count = 0;
			int featureLen = -1;
			for (Row row : values) {
				count++;
				if (tensorColName != null) {
					Object obj = row.getField(0);
					Vector vec = VectorUtil.getVector(obj);
					if (vec instanceof SparseVector) {

						int[] indices = ((SparseVector) vec).getIndices();
						featureLen = Math.max(featureLen,
							(indices.length == 0 ? -1 : indices[indices.length - 1]) + 1);
					} else {
						featureLen = vec.size();
					}
				} else {
					featureLen = this.featureColnames.length;
				}
			}
			out.collect(Tuple3.of(count, featureLen, parallel));
		}
	}
}


