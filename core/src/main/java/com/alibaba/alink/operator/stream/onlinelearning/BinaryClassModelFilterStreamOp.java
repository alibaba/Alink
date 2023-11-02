package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.onlinelearning.BinaryClassModelFilterParams;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.buildLabelIndexLabelArray;

/**
 * filter stream model with special condition, just as auc or accuracy larger than given threshold.
 */

@Internal
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL_STREAM, opType = OpType.SAME),
	@PortSpec(value = PortType.DATA, opType = OpType.SAME),
})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL_STREAM)})
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("二分类模型过滤")
public class BinaryClassModelFilterStreamOp<T extends BinaryClassModelFilterStreamOp<T>> extends StreamOperator <T> {

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	private final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public BinaryClassModelFilterStreamOp(TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
										  Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		checkOpSize(2, inputs);
		this.getParams().set(HasPredictionCol.PREDICTION_COL, "alink_inner_pred")
			.set(EvalBinaryClassParams.PREDICTION_DETAIL_COL, "alink_inner_detail")
			.set(HasReservedColsDefaultAsNull.RESERVED_COLS,
				new String[] {getParams().get(HasLabelCol.LABEL_COL)});

		try {
			TableSchema modelStreamSchema = inputs[0].getSchema();

			final int timestampColIndex = ModelStreamUtils.findTimestampColIndexWithAssertAndHint(modelStreamSchema);
			final int countColIndex = ModelStreamUtils.findCountColIndexWithAssertAndHint(modelStreamSchema);
			TableSchema modelSchema = ModelStreamUtils.getRawModelSchema(modelStreamSchema, timestampColIndex,
				countColIndex);

			/* predict samples */
			DataStream <Row> evalResults = inputs[0].getDataStream()
				.connect(inputs[1].getDataStream())
				.flatMap(
					new FilterProcess(
						modelSchema, inputs[1].getSchema(), this.getParams(),
						this.mapperBuilder, timestampColIndex, countColIndex
					)
				)
				.setParallelism(1);

			this.setOutput(evalResults, inputs[0].getSchema());

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkUnclassifiedErrorException(ex.toString());
		}

		return (T) this;
	}

	public static class FilterProcess extends RichCoFlatMapFunction <Row, Row, Row> {

		private final String positiveValue;
		private final List <Row> bufferRows = new ArrayList <>();
		private final double aucThreshold;
		private final double accuracyThreshold;
		private final double logLossThreshold;
		private final TypeInformation<?> labelType;

		private final ModelMapper mapper;
		private final Map <Timestamp, List <Row>> buffers = new HashMap <>(0);
		private final int timestampColIndex;
		private final int countColIndex;

		private Long modelBid = 0L;
		private Long evalID = 1L;

		/* eval info */
		private Object[] recordLabel = null;
		private List <Row> model;
		private List <Row> oldModel;

		private transient BinaryMetricsSummary binaryMetricsSummary;
		private transient long[] bin0;
		private transient long[] bin1;

		public FilterProcess(TableSchema modelSchema, TableSchema dataSchema, Params params,
							 TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
							 int timestampColIndex, int countColIndex) {
			this.mapper = mapperBuilder.apply(modelSchema, dataSchema, params);

			this.labelType = modelSchema.getFieldTypes()[modelSchema.getFieldTypes().length-1];
			this.positiveValue = params.get(BinaryClassModelFilterParams.POS_LABEL_VAL_STR);
			this.aucThreshold = params.get(BinaryClassModelFilterParams.AUC_THRESHOLD);
			this.accuracyThreshold = params.get(BinaryClassModelFilterParams.ACCURACY_THRESHOLD);
			this.logLossThreshold = params.get(BinaryClassModelFilterParams.LOG_LOSS_THRESHOLD);
			this.timestampColIndex = timestampColIndex;
			this.countColIndex = countColIndex;
		}

		@Override
		public void flatMap2(Row row, Collector <Row> collector) throws Exception {
			if (modelBid.equals(0L)) {
				bufferRows.add(row);
				return;
			}
			evalRow(row, collector);
		}

		private void evalRow(Row row, Collector <Row> collector) throws Exception {
			Row pred = RowUtil.remove(this.mapper.map(row), 1);

			if (bin0 == null) {
				bin0 = new long[ClassificationEvaluationUtil.DETAIL_BIN_NUMBER];
				bin1 = new long[ClassificationEvaluationUtil.DETAIL_BIN_NUMBER];
			}
			if (EvaluationUtil.checkRowFieldNotNull(row)) {
				TreeMap <Object, Double> labelProbMap = EvaluationUtil.extractLabelProbMap(pred, labelType);
				// Get the label info from detail col, the operation is done only at the first row.
				if (null == recordLabel) {
					recordLabel = buildLabelIndexLabelArray(new HashSet <>(labelProbMap.keySet()), true,
						positiveValue, labelType, true).f1;
					binaryMetricsSummary = new BinaryMetricsSummary(bin0, bin1, recordLabel, 0.0, 0L);
				}
				EvaluationUtil.updateBinaryMetricsSummary(EvaluationUtil.extractLabelProbMap(pred, labelType),
					pred.getField(0), binaryMetricsSummary);
			}

			if (modelBid > evalID) {
				if (null != recordLabel) {
					BinaryClassMetrics metrics = binaryMetricsSummary.toMetrics();
					double auc = metrics.getAuc();
					double accuracy = metrics.getAccuracy();
					double logLoss = metrics.getLogLoss();

					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("auc : " + auc + "     accuracy : " + accuracy + "    logLoss : " + logLoss);
					}
					if (auc >= aucThreshold && accuracy >= accuracyThreshold && logLoss < logLossThreshold) {
						for (Row r : oldModel) {
							collector.collect(r);
						}
					}
				}
				evalID++;
				recordLabel = null;
				for (int i = 0; i < ClassificationEvaluationUtil.DETAIL_BIN_NUMBER; ++i) {
					bin0[i] = 0L;
					bin1[i] = 0L;
				}
			}
		}

		@Override
		public void flatMap1(Row inRow, Collector <Row> collector)
			throws Exception {
			Timestamp id = (Timestamp) inRow.getField(timestampColIndex);
			long nTab = (long) inRow.getField(countColIndex);

			if (buffers.containsKey(id) && buffers.get(id).size() == (int) nTab - 1) {

				if (buffers.containsKey(id)) {
					buffers.get(id).add(inRow);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(inRow);
					buffers.put(id, buffer);
				}

				oldModel = model;
				model = buffers.get(id);

				ArrayList <Row> tempRows = new ArrayList <>();
				for (Row r : model) {
					tempRows.add(ModelStreamUtils.genRowWithoutIdentifier(r, timestampColIndex, countColIndex));
				}
				this.mapper.loadModel(tempRows);

				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("load model : " + modelBid);
				}
				modelBid++;
				if (bufferRows.size() != 0) {
					for (Row row1 : bufferRows) {
						evalRow(row1, collector);
					}
					bufferRows.clear();
				}

				buffers.remove(id);
			} else {
				if (buffers.containsKey(id)) {
					buffers.get(id).add(inRow);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(inRow);
					buffers.put(id, buffer);
				}
			}

		}
	}
}
