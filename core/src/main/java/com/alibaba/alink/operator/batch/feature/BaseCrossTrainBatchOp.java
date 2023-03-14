package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.FirstReducer;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.operator.common.slidingwindow.SessionSharedData;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.feature.AutoCrossTrainParams;
import com.alibaba.alink.params.feature.HasDropLast;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.feature.AutoCrossAlgoModel;
import com.alibaba.alink.pipeline.feature.OneHotEncoderModel;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.batch.feature.AutoCrossTrainBatchOp.AC_TRAIN_DATA;
import static com.alibaba.alink.operator.batch.feature.AutoCrossTrainBatchOp.SESSION_ID;

@ReservedColsWithFirstInputSpec
@ParamSelectColumnSpec(name = "selectedCols")
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("")
public abstract class BaseCrossTrainBatchOp<T extends BaseCrossTrainBatchOp <T>>
	extends BatchOperator <T>
	implements AutoCrossTrainParams <T> {

	static final String oneHotVectorCol = "oneHotVectorCol";
	boolean hasDiscrete = true;

	BaseCrossTrainBatchOp(Params params) {
		super(params);
	}

	//todo construct function.

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator in = checkAndGetFirst(inputs);

		String[] reversedCols = getParams().get(HasReservedColsDefaultAsNull.RESERVED_COLS);
		if (reversedCols == null) {
			reversedCols = in.getColNames();
		}

		long mlEnvId = getMLEnvironmentId();

		String[] featureCols = getSelectedCols();
		final String labelCol = getLabelCol();

		String[] selectedCols = ArrayUtils.add(featureCols, labelCol);
		in = in.select(selectedCols);
		TableSchema inputSchema = in.getSchema();
		final ExecutionEnvironment env = MLEnvironmentFactory.get(mlEnvId).getExecutionEnvironment();

		String[] categoricalCols = TableUtil.getCategoricalCols(in.getSchema(), featureCols,
			getParams().contains(RandomForestTrainParams.CATEGORICAL_COLS) ?
				getParams().get(RandomForestTrainParams.CATEGORICAL_COLS) : null
		);
		if (null == categoricalCols || categoricalCols.length == 0) {
			throw new AkIllegalArgumentException("Please input param CategoricalCols!");
		}
		String[] numericalCols = ArrayUtils.removeElements(featureCols, categoricalCols);

		Params oneHotParams = new Params().set(AutoCrossTrainParams.SELECTED_COLS, categoricalCols);
		if (getParams().contains(AutoCrossTrainParams.DISCRETE_THRESHOLDS_ARRAY)) {
			oneHotParams.set(AutoCrossTrainParams.DISCRETE_THRESHOLDS_ARRAY, getDiscreteThresholdsArray());
		} else if (getParams().contains(AutoCrossTrainParams.DISCRETE_THRESHOLDS)) {
			oneHotParams.set(AutoCrossTrainParams.DISCRETE_THRESHOLDS, getDiscreteThresholds());
		}
		oneHotParams.set(HasDropLast.DROP_LAST, false)
			.set(HasOutputColsDefaultAsNull.OUTPUT_COLS, new String[] {oneHotVectorCol});

		OneHotTrainBatchOp oneHotModel = new OneHotTrainBatchOp(oneHotParams)
			.setMLEnvironmentId(mlEnvId)
			.linkFrom(in);

		OneHotEncoderModel oneHotEncoderModel = new OneHotEncoderModel(oneHotParams)
			.setMLEnvironmentId(mlEnvId);
		oneHotEncoderModel.setModelData(oneHotModel);

		//todo first do not train numerical model.
		//if (numericalCols.size() != 0) {
		//	Params numericalParams = new Params()
		//		.set(AutoCrossTrainParams.SELECTED_COLS, numericalCols.toArray(new String[0]));
		//	if (getParams().contains(AutoCrossTrainParams.NUM_BUCKETS_ARRAY)) {
		//		numericalParams.set(AutoCrossTrainParams.NUM_BUCKETS_ARRAY, getNumBucketsArray());
		//	} else if (getParams().contains(AutoCrossTrainParams.NUM_BUCKETS)) {
		//		numericalParams.set(AutoCrossTrainParams.NUM_BUCKETS, getNumBuckets());
		//	}
		//
		//	BatchOperator quantile;
		//	if (getBinningMethod().equals(BinningMethod.QUANTILE)) {
		//		quantile = new QuantileDiscretizerTrainBatchOp(numericalParams)
		//			.linkFrom(in);
		//	} else {
		//		quantile = new EqualWidthDiscretizerTrainBatchOp(numericalParams)
		//			.linkFrom(in);
		//	}
		//	QuantileDiscretizerModel numericalModel = BinningTrainBatchOp
		//		.setQuantileDiscretizerModelData(
		//			quantile,
		//			numericalCols.toArray(new String[0]),
		//			getParams().get(ML_ENVIRONMENT_ID));
		//	listModel.add(numericalModel);
		//}

		TransformerBase <?>[] finalModel = new TransformerBase[2];
		finalModel[0] = oneHotEncoderModel;

		in = new OneHotPredictBatchOp(oneHotParams)
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(oneHotModel, in);

		hasDiscrete = OneHotModelMapper.isEnableElse(oneHotParams);

		DataSet <int[]> featureSizeDataSet = env.fromElements(new int[0])
			.map(new BuildFeatureSize(hasDiscrete))
			.withBroadcastSet(oneHotModel.getDataSet(), "oneHotModel");

		DataSet <Object> positiveLabel = in
			.select(labelCol)
			.getDataSet().reduceGroup(new FirstReducer(1))
			.map(new MapFunction <Row, Object>() {
				private static final long serialVersionUID = 110081999458221448L;

				@Override
				public Object map(Row value) throws Exception {
					return value.getField(0);
				}
			});

		int svIndex = TableUtil.findColIndex(in.getColNames(), oneHotVectorCol);
		int labelIndex = TableUtil.findColIndex(in.getColNames(), labelCol);

		int[] numericalIndices = TableUtil.findColIndicesWithAssert(in.getSchema(), numericalCols);
		DataColumnsSaver dataColumnsSaver = new DataColumnsSaver(categoricalCols, numericalCols, numericalIndices);

		//here numerical cols is concatted first.
		DataSet <Tuple2 <Integer, Tuple3 <Double, Double, Vector>>> trainDataOrigin =
			in.getDataSet().rebalance()
				.mapPartition(new GetTrainData(numericalIndices, svIndex, labelIndex))
				.withBroadcastSet(positiveLabel, "positiveLabel")
				.partitionCustom(new Partitioner <Integer>() {
					private static final long serialVersionUID = 5552966434608252752L;

					@Override
					public int partition(Integer key, int numPartitions) {
						return key;
					}
				}, 0);

		DataSet <Tuple3 <Double, Double, Vector>> trainData = trainDataOrigin
			.mapPartition(
				new RichMapPartitionFunction <Tuple2 <Integer, Tuple3 <Double, Double, Vector>>,
					Tuple3 <Double, Double, Vector>>() {
					@Override
					public void mapPartition(Iterable <Tuple2 <Integer, Tuple3 <Double, Double, Vector>>> values,
											 Collector <Tuple3 <Double, Double, Vector>> out)
						throws Exception {
						List <Tuple3 <Double, Double, Vector>> samples = new ArrayList <>();
						for (Tuple2 <Integer, Tuple3 <Double, Double, Vector>> value : values) {
							samples.add(value.f1);
						}
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						SessionSharedData.put(AC_TRAIN_DATA, SESSION_ID, taskId, samples);
					}
				});

		DataSet <Row> acModel = buildAcModelData(trainData, featureSizeDataSet, dataColumnsSaver);

		BatchOperator <?> autoCrossBatchModel = BatchOperator
			.fromTable(
				DataSetConversionUtil.toTable(mlEnvId,
					acModel, new String[] {"feature_id", "cross_feature", "score"},
					new TypeInformation[] {Types.LONG, Types.STRING, Types.DOUBLE}));

		Params autoCrossParams = getParams();
		autoCrossParams.set(HasReservedColsDefaultAsNull.RESERVED_COLS, reversedCols);
		AutoCrossAlgoModel acPipelineModel = new AutoCrossAlgoModel(autoCrossParams)
			.setModelData(autoCrossBatchModel)
			.setMLEnvironmentId(mlEnvId);
		finalModel[1] = acPipelineModel;

		BatchOperator <?> modelSaved = new PipelineModel(finalModel).save();

		DataSet <Row> modelRows = modelSaved
			.getDataSet()
			.map(new PipelineModelMapper.ExtendPipelineModelRow(selectedCols.length + 1));

		setOutput(modelRows, getAutoCrossModelSchema(inputSchema, modelSaved.getSchema(), selectedCols));

		buildSideOutput(oneHotModel, acModel, Arrays.asList(numericalCols), mlEnvId);

		return (T) this;
	}

	abstract DataSet <Row> buildAcModelData(DataSet <Tuple3 <Double, Double, Vector>> trainData,
											DataSet <int[]> featureSizeDataSet,
											DataColumnsSaver dataColumnsSaver);

	abstract void buildSideOutput(OneHotTrainBatchOp oneHotModel, DataSet <Row> acModel,
								  List <String> numericalCols, long mlEnvId);

	static class DataColumnsSaver {
		String[] categoricalCols;
		String[] numericalCols;
		int[] numericalIndices;

		DataColumnsSaver(String[] categoricalCols,
						 String[] numericalCols,
						 int[] numericalIndices) {
			this.categoricalCols = categoricalCols;
			this.numericalCols = numericalCols;
			this.numericalIndices = numericalIndices;
		}
	}

	public static class BuildFeatureSize extends RichMapFunction <int[], int[]> {
		private static final long serialVersionUID = 873642749154257046L;
		private final int additionalSize;
		private int[] featureSize;

		BuildFeatureSize(boolean hasDiscrete) {
			additionalSize = hasDiscrete ? 2 : 1;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			List <Row> modelRow = getRuntimeContext().getBroadcastVariable("oneHotModel");
			MultiStringIndexerModelData data = new OneHotModelDataConverter().load(modelRow).modelData;
			int featureNumber = data.tokenNumber.size();
			featureSize = new int[featureNumber];
			for (int i = 0; i < featureNumber; i++) {
				featureSize[i] = (int) (data.tokenNumber.get(i) + additionalSize);
			}
		}

		@Override
		public int[] map(int[] value) throws Exception {
			return featureSize;
		}
	}

	//concat numerical cols and the onehot sv.
	//todo note: cannot have null value in the numerical data.
	public static class GetTrainData
		extends RichMapPartitionFunction <Row, Tuple2 <Integer, Tuple3 <Double, Double, Vector>>> {
		private static final long serialVersionUID = -4406174781328407356L;
		private int[] numericalIndices;
		private int svIndex;
		private int labelIndex;
		private Object positiveLabel;

		GetTrainData(int[] numericalIndices, int svIndex, int labelIndex) {
			this.svIndex = svIndex;
			this.labelIndex = labelIndex;
			this.numericalIndices = numericalIndices;
		}

		@Override
		public void mapPartition(Iterable <Row> values,
								 Collector <Tuple2 <Integer, Tuple3 <Double, Double, Vector>>> out) throws Exception {
			int taskNum = getRuntimeContext().getNumberOfParallelSubtasks();
			int vecSize = -1;
			int svSize = -1;
			int[] vecIndices = null;
			double[] vecValues = null;
			SparseVector sv;
			for (Row rowData : values) {
				if (vecSize == -1) {
					sv = VectorUtil.getSparseVector(rowData.getField(svIndex));
					vecSize = numericalIndices.length + sv.getIndices().length;
					svSize = numericalIndices.length + sv.size();
					vecIndices = new int[vecSize];
					vecValues = new double[vecSize];
				}
				for (int i = 0; i < numericalIndices.length; i++) {
					vecIndices[i] = i;
					vecValues[i] = ((Number) rowData.getField(numericalIndices[i])).doubleValue();
				}
				sv = VectorUtil.getSparseVector(rowData.getField(svIndex));
				int[] svIndices = new int[sv.getIndices().length];
				for (int i = 0; i < svIndices.length; i++) {
					svIndices[i] = sv.getIndices()[i] + numericalIndices.length;
				}
				System.arraycopy(svIndices, 0, vecIndices, numericalIndices.length, sv.getIndices().length);
				System.arraycopy(sv.getValues(), 0, vecValues, numericalIndices.length, sv.getValues().length);

				for (int i = 0; i < taskNum; i++) {
					out.collect(Tuple2.of(i, Tuple3.of(
						1.0,
						positiveLabel.equals(rowData.getField(labelIndex)) ? 1. : 0.,
						new SparseVector(svSize, vecIndices, vecValues))));
				}

			}
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			positiveLabel = getRuntimeContext().getBroadcastVariable("positiveLabel").get(0);
		}
	}

	public static TableSchema getAutoCrossModelSchema(
		TableSchema dataSchema, TableSchema modelSchema, String[] selectedCols) {

		int pipeFieldCount = modelSchema.getFieldNames().length;
		String[] modelCols = new String[pipeFieldCount + 1 + selectedCols.length];
		TypeInformation <?>[] modelType = new TypeInformation[pipeFieldCount + 1 + selectedCols.length];
		System.arraycopy(modelSchema.getFieldNames(), 0, modelCols, 0, pipeFieldCount);
		System.arraycopy(modelSchema.getFieldTypes(), 0, modelType, 0, pipeFieldCount);
		modelCols[pipeFieldCount] = PipelineModelMapper.SPLITER_COL_NAME;
		modelType[pipeFieldCount] = PipelineModelMapper.SPLITER_COL_TYPE;
		System.arraycopy(selectedCols, 0, modelCols, pipeFieldCount + 1, selectedCols.length);
		for (int i = 0; i < selectedCols.length; i++) {
			int index = TableUtil.findColIndex(dataSchema, selectedCols[i]);
			modelType[i + pipeFieldCount + 1] = dataSchema.getFieldTypes()[index];
		}
		return new TableSchema(modelCols, modelType);
	}
}
