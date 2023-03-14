package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.onlinelearning.BinaryClassModelFilterParams;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.buildLabelIndexLabelArray;
import static com.alibaba.alink.pipeline.ModelExporterUtils.loadMapperListFromStages;
import static com.alibaba.alink.pipeline.ModelExporterUtils.loadStagesFromPipelineModel;

/**
 * filter stream model with special condition, just as auc or accuracy larger than given threshold.
 */

@Internal
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL_STREAM, opType = OpType.SAME),
	@PortSpec(value = PortType.DATA, opType = OpType.SAME),
})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL_STREAM)})
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("Pipeline二分类模型过滤")
public class BinaryClassPipelineModelFilterStreamOp extends StreamOperator <BinaryClassPipelineModelFilterStreamOp>
	implements BinaryClassModelFilterParams <BinaryClassPipelineModelFilterStreamOp> {

	public BinaryClassPipelineModelFilterStreamOp() {
		super();
	}

	public BinaryClassPipelineModelFilterStreamOp(Params params) {
		super(params);
	}

	@Override
	public BinaryClassPipelineModelFilterStreamOp linkFrom(StreamOperator <?>... inputs) {
		checkOpSize(2, inputs);

		try {
			TableSchema modelStreamSchema = inputs[0].getSchema();

			final int timestampColIndex = ModelStreamUtils.findTimestampColIndexWithAssertAndHint(modelStreamSchema);
			final int countColIndex = ModelStreamUtils.findCountColIndexWithAssertAndHint(modelStreamSchema);
			TableSchema modelSchema = ModelStreamUtils.getRawModelSchema(modelStreamSchema, timestampColIndex,
				countColIndex);

			TypeInformation <?> labelType = inputs[1].getSchema().getFieldTypes()[
				TableUtil.findColIndex(inputs[1].getSchema(), getLabelCol())];
			/* predict samples */
			DataStream <Row> evalResults = inputs[0].getDataStream()
				.connect(inputs[1].getDataStream())
				.flatMap(
					new FilterProcess(
						modelSchema, inputs[1].getSchema(), this.getParams(),
						timestampColIndex, countColIndex, labelType)
				)
				.setParallelism(1);

			this.setOutput(evalResults, inputs[0].getSchema());

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkUnclassifiedErrorException(ex.toString());
		}

		return this;
	}

	public static class FilterProcess extends RichCoFlatMapFunction <Row, Row, Row> {

		private final String positiveValue;
		private final double aucThreshold;
		private final double accuracyThreshold;
		private final double logLossThreshold;
		private final TypeInformation <?> labelType;

		private final String modelSchemaStr;
		private final String dataSchemaStr;
		private final String labelCol;
		private String predDetailCol;
		private int indexLabelCol;
		private int indexPredDetailCol;

		private final Map <Timestamp, List <Row>> buffers = new HashMap <>(0);
		private final int timestampColIndex;
		private final int countColIndex;
		private MapperChain mapperChain;

		private Long modelBid = 0L;

		/* eval info */
		private Object[] recordLabel = null;

		private transient BinaryMetricsSummary binaryMetricsSummary;
		private transient long[] bin0;
		private transient long[] bin1;
		Queue <Row> queue = new LinkedList();
		private final int evalBatchSize;
		public FilterProcess(TableSchema modelSchema, TableSchema dataSchema, Params params,
							 int timestampColIndex, int countColIndex, TypeInformation <?> labelType) {
			this.modelSchemaStr = TableUtil.schema2SchemaStr(modelSchema);
			this.dataSchemaStr = TableUtil.schema2SchemaStr(dataSchema);
			this.labelCol = params.get(EvalBinaryClassParams.LABEL_COL);

			this.positiveValue = params.get(BinaryClassModelFilterParams.POS_LABEL_VAL_STR);
			this.aucThreshold = params.get(BinaryClassModelFilterParams.AUC_THRESHOLD);
			this.accuracyThreshold = params.get(BinaryClassModelFilterParams.ACCURACY_THRESHOLD);
			this.logLossThreshold = params.get(BinaryClassModelFilterParams.LOG_LOSS);
			this.evalBatchSize = params.get(BinaryClassModelFilterParams.NUM_EVAL_SAMPLES);
			this.timestampColIndex = timestampColIndex;
			this.countColIndex = countColIndex;
			this.labelType = labelType;
		}

		@Override
		public void flatMap2(Row row, Collector <Row> collector) throws Exception {
			queue.add(row);
			if (queue.size() > evalBatchSize) {
				queue.remove();
			}
		}

		private void evalRow(Row row) throws Exception {

			if (null == this.mapperChain) {return;}

			Row t = this.mapperChain.map(row);
			Row pred = Row.of(t.getField(this.indexLabelCol), t.getField(this.indexPredDetailCol));

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
				List <Row> model = buffers.get(id);

				ArrayList <Row> tempRows = new ArrayList <>();
				for (Row r : model) {
					tempRows.add(ModelStreamUtils.genRowWithoutIdentifier(r, timestampColIndex, countColIndex));
				}

				TableSchema outputSchema = loadPipelineModel(tempRows);
				indexLabelCol = TableUtil.findColIndex(outputSchema, this.labelCol);
				indexPredDetailCol = TableUtil.findColIndex(outputSchema, this.predDetailCol);

				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("load model : " + modelBid);
				}
				modelBid++;
				if (queue.size() != 0) {
					for (Row row : queue) {
						evalRow(row);
					}
				}

				if (null != recordLabel) {
					BinaryClassMetrics metrics = binaryMetricsSummary.toMetrics();
					double auc = metrics.getAuc();
					double accuracy = metrics.getAccuracy();
					double logLoss = metrics.getLogLoss();

					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("auc : " + auc + "     accuracy : " + accuracy + "    logLoss : " + logLoss);
					}
					if (auc >= aucThreshold && accuracy >= accuracyThreshold && logLoss < logLossThreshold) {
						for (Row r : model) {
							collector.collect(r);
						}
					}
				}
				recordLabel = null;
				for (int i = 0; i < ClassificationEvaluationUtil.DETAIL_BIN_NUMBER; ++i) {
					bin0[i] = 0L;
					bin1[i] = 0L;
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

		private TableSchema loadPipelineModel(List <Row> modelRows) {
			List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> stages =
				loadStagesFromPipelineModel(modelRows, TableUtil.schemaStr2Schema(modelSchemaStr));
			Params classParams = stages.get(stages.size() - 1).f0.getParams();
			classParams.set(HasReservedColsDefaultAsNull.RESERVED_COLS, new String[] {labelCol});
			this.predDetailCol = classParams.get(HasPredictionDetailCol.PREDICTION_DETAIL_COL);
			this.mapperChain = loadMapperListFromStages(stages, TableUtil.schemaStr2Schema(dataSchemaStr));
			this.mapperChain.open();
			return this.mapperChain.getMappers()[mapperChain.getMappers().length - 1].getOutputSchema();
		}
	}
}
