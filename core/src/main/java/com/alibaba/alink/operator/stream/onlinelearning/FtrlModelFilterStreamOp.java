package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.FtrlModelFilterParams;
import com.alibaba.alink.params.onlinelearning.FtrlPredictParams;

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
public final class FtrlModelFilterStreamOp extends StreamOperator <FtrlModelFilterStreamOp>
	implements FtrlModelFilterParams <FtrlModelFilterStreamOp> {

	private static final long serialVersionUID = 3299632572961991298L;

	public FtrlModelFilterStreamOp() {
		super(new Params());
	}

	public FtrlModelFilterStreamOp(Params params) {
		super(params);
	}

	@Override
	public FtrlModelFilterStreamOp linkFrom(StreamOperator <?>... inputs) {
		checkOpSize(2, inputs);
		this.getParams().set(FtrlPredictParams.PREDICTION_COL, "alink_inner_pred")
			.set(FtrlPredictParams.PREDICTION_DETAIL_COL, "alink_inner_detail")
			.set(FtrlPredictParams.RESERVED_COLS, new String[] {getLabelCol()});

		String positiveValue = getPositiveLabelValueString();

		try {
			DataStream <Tuple2 <Long, LinearModelData>> modelstr = inputs[0].getDataStream().flatMap(
				new CollectModel());
			TypeInformation[] types = new TypeInformation[3];
			String[] names = new String[3];
			TableSchema streamModelSchema = inputs[0].getSchema();
			for (int i = 0; i < 3; ++i) {
				names[i] = streamModelSchema.getFieldNames()[i + 2];
				types[i] = streamModelSchema.getFieldTypes()[i + 2];
			}
			TableSchema modelSchema = new TableSchema(names, types);
			/* predict samples */
			DataStream <Tuple2 <Long, LinearModelData>> evalResults = modelstr.connect(inputs[1].getDataStream())
				.flatMap(new FilterProcess(modelSchema, inputs[1].getSchema(), this.getParams(), positiveValue))
				.setParallelism(1);

			DataStream <Row> result = evalResults.flatMap(new FlatMapFunction <Tuple2 <Long, LinearModelData>, Row>() {
				private static final long serialVersionUID = 8595724722300108190L;

				@Override
				public void flatMap(Tuple2 <Long, LinearModelData> model, Collector <Row> out)
					throws Exception {
					RowCollector listCollector = new RowCollector();
					new LinearModelDataConverter().save(model.f1, listCollector);
					List <Row> rows = listCollector.getRows();
					for (Row r : rows) {
						int rowSize = r.getArity();
						Row row = new Row(rowSize + 2);
						row.setField(0, model.f0);
						row.setField(1, rows.size() + 0L);

						for (int j = 0; j < rowSize; ++j) {
							if (j == 2 && r.getField(j) != null) {
								row.setField(2 + j, r.getField(j));
							} else {
								row.setField(2 + j, r.getField(j));
							}
						}
						out.collect(row);
					}
				}
			});
			this.setOutput(result, inputs[0].getSchema());

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException(ex.toString());
		}

		return this;
	}

	public static class CollectModel implements FlatMapFunction <Row, Tuple2 <Long, LinearModelData>> {

		private static final long serialVersionUID = 8975618972398612123L;
		private Map <Long, List <Row>> buffers = new HashMap <>(0);

		@Override
		public void flatMap(Row inRow, Collector <Tuple2 <Long, LinearModelData>> out) throws Exception {
			long id = (long) inRow.getField(0);
			Long nTab = (long) inRow.getField(1);

			Row row = new Row(inRow.getArity() - 2);

			for (int i = 0; i < row.getArity(); ++i) {
				row.setField(i, inRow.getField(i + 2));
			}

			if (buffers.containsKey(id) && buffers.get(id).size() == nTab.intValue() - 1) {

				if (buffers.containsKey(id)) {
					buffers.get(id).add(row);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(row);
					buffers.put(id, buffer);
				}

				LinearModelData ret = new LinearModelDataConverter().load(buffers.get(id));
				buffers.get(id).clear();
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("collect model : " + id);
				}
				out.collect(Tuple2.of(id, ret));
			} else {
				if (buffers.containsKey(id)) {
					buffers.get(id).add(row);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(row);
					buffers.put(id, buffer);
				}
			}
		}
	}

	public static class FilterProcess
		extends RichCoFlatMapFunction <Tuple2 <Long, LinearModelData>, Row, Tuple2 <Long, LinearModelData>> {

		private static final long serialVersionUID = 2482980204464010971L;
		private LinearModelMapper predictor = null;
		private final String[] dataFieldNames;
		private final DataType[] dataFieldTypes;
		private final String[] modelFieldNames;
		private final DataType[] modelFieldTypes;
		private Params params;
		private Long modelBid = 0L;
		private Long evalID = 1L;

		/* eval info */
		private Object[] recordLabel = null;
		private transient BinaryMetricsSummary binaryMetricsSummary;
		private transient long[] bin0;
		private transient long[] bin1;
		private String positiveValue;
		private List <Row> bufferRows = new ArrayList <>();
		Tuple2 <Long, LinearModelData> model;
		Tuple2 <Long, LinearModelData> oldModel;
		private double aucThreshold;
		private double accuracyThreshold;
		private TypeInformation labelType;

		public FilterProcess(TableSchema modelSchema, TableSchema dataSchema, Params params,
							 String positiveValue) {
			this.dataFieldNames = dataSchema.getFieldNames();
			this.dataFieldTypes = dataSchema.getFieldDataTypes();
			this.modelFieldNames = modelSchema.getFieldNames();
			this.modelFieldTypes = modelSchema.getFieldDataTypes();
			this.params = params;
			this.positiveValue = positiveValue;
			this.aucThreshold = params.get(FtrlModelFilterParams.AUC_THRESHOLD);
			this.accuracyThreshold = params.get(FtrlModelFilterParams.ACCURACY_THRESHOLD);
		}

		protected TableSchema getDataSchema() {
			return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
		}

		protected TableSchema getModelSchema() {
			return TableSchema.builder().fields(modelFieldNames, modelFieldTypes).build();
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			TableSchema modelSchema = getModelSchema();
			this.predictor = new LinearModelMapper(modelSchema,
				getDataSchema(), this.params);
			labelType = modelSchema.getFieldTypes()[2];
		}

		@Override
		public void flatMap2(Row row, Collector <Tuple2 <Long, LinearModelData>> collector) throws Exception {
			if (modelBid.equals(0L)) {
				bufferRows.add(row);
				return;
			}
			evalRow(row, collector);
		}

		private void evalRow(Row row, Collector <Tuple2 <Long, LinearModelData>> collector) throws Exception {
			Row pred = RowUtil.remove(this.predictor.map(row), 1);

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
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("auc : " + auc + "     accuracy : " + accuracy);
					}
					if (auc > aucThreshold && accuracy > accuracyThreshold) {
						collector.collect(oldModel);
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
		public void flatMap1(Tuple2 <Long, LinearModelData> linearModel,
							 Collector <Tuple2 <Long, LinearModelData>> collector)
			throws Exception {
			oldModel = model;
			model = linearModel;
			this.predictor.loadModel(linearModel.f1);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("load model : " + modelBid);
			}
			modelBid++;
			if (bufferRows.size() != 0) {
				for (Row row : bufferRows) {
					evalRow(row, collector);
				}
				bufferRows.clear();
			}
		}
	}
}
