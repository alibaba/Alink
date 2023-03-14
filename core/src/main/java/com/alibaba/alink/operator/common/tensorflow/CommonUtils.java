package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelData;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelDataConverter;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelData;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelDataConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class CommonUtils {
	public static final String TF_MODEL_BC_NAME = "TF_MODEL";
	public static final String PREPROCESS_PIPELINE_MODEL_BC_NAME = "PREPROCESS_PIPELINE_MODEL";
	public static final String SORTED_LABELS_BC_NAME = "SORTED_LABELS";

	public static BatchOperator <?> mapLabelToIndex(BatchOperator <?> in, String labelCol,
													DataSet <List <Object>> sortedLabels) {
		TableSchema schema = in.getSchema();
		int labelColId = TableUtil.findColIndexWithAssertAndHint(schema, labelCol);
		DataSet <Row> mappedDataSet = in.getDataSet()
			.map(new LabelToIndexMapper(labelColId))
			.withBroadcastSet(sortedLabels, SORTED_LABELS_BC_NAME);
		String[] fieldNames = schema.getFieldNames();
		TypeInformation <?>[] fieldTypes = schema.getFieldTypes();
		fieldTypes[labelColId] = Types.FLOAT;
		TableSchema mappedSchema = new TableSchema(fieldNames, fieldTypes);
		return BatchOperator.fromTable(
				DataSetConversionUtil.toTable(in.getMLEnvironmentId(), mappedDataSet, mappedSchema))
			.setMLEnvironmentId(in.getMLEnvironmentId());
	}

	public static class SortLabelsReduceGroupFunction implements GroupReduceFunction <Row, List <Object>> {
		@Override
		public void reduce(Iterable <Row> values, Collector <List <Object>> out) throws Exception {
			TreeSet <Object> labelSet = new TreeSet <>();
			for (Row value : values) {
				labelSet.add(value.getField(0));
			}
			List <Object> sortedLabels = new ArrayList <>(labelSet);
			out.collect(sortedLabels);
		}
	}

	public static class CountLabelsMapFunction implements MapFunction <List <Object>, Row> {
		@Override
		public Row map(List <Object> value) throws Exception {
			return Row.of(value.size());
		}
	}

	public static class ConstructModelFlatMapFunction extends RichFlatMapFunction <Row, Row> {

		private final Params params;
		private final String[] featureCols;
		private final String tfOutputSignatureDef;
		private final TypeInformation <?> tfOutputSignatureType;
		private final String preprocessPipelineModelSchemaStr;

		// Indicate the output tensor are logits or not; ignored in regression models.
		private final boolean isOutputLogits;

		private List <Row> preprocessPipelineModelRows;
		private List <Row> tfModelRows;
		private List <Object> sortedLabels;

		public ConstructModelFlatMapFunction(Params params, String[] featureCols,
											 String tfOutputSignatureDef,
											 TypeInformation <?> tfOutputSignatureType,
											 String preprocessPipelineModelSchemaStr) {
			this(params, featureCols, tfOutputSignatureDef, tfOutputSignatureType,
				preprocessPipelineModelSchemaStr, false);
		}

		public ConstructModelFlatMapFunction(Params params, String[] featureCols,
											 String tfOutputSignatureDef,
											 TypeInformation <?> tfOutputSignatureType,
											 String preprocessPipelineModelSchemaStr,
											 boolean isOutputLogits) {
			this.params = params;
			this.featureCols = featureCols;
			this.tfOutputSignatureDef = tfOutputSignatureDef;
			this.tfOutputSignatureType = tfOutputSignatureType;
			this.preprocessPipelineModelSchemaStr = preprocessPipelineModelSchemaStr;
			this.isOutputLogits = isOutputLogits;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			RuntimeContext runtimeContext = getRuntimeContext();
			sortedLabels = runtimeContext.hasBroadcastVariable(SORTED_LABELS_BC_NAME)
				? runtimeContext. <List <Object>>getBroadcastVariable(SORTED_LABELS_BC_NAME).get(0)
				: Collections.emptyList();
			tfModelRows = runtimeContext.getBroadcastVariable(TF_MODEL_BC_NAME);

			preprocessPipelineModelRows = runtimeContext.hasBroadcastVariable(PREPROCESS_PIPELINE_MODEL_BC_NAME)
				? runtimeContext.getBroadcastVariable(PREPROCESS_PIPELINE_MODEL_BC_NAME)
				: Collections.emptyList();
		}

		@Override
		public void flatMap(Row value, Collector <Row> out) throws Exception {
			if (sortedLabels.size() > 0) {
				TFTableModelClassificationModelData modelData =
					new TFTableModelClassificationModelData(params, featureCols, tfModelRows,
						tfOutputSignatureDef, tfOutputSignatureType,
						preprocessPipelineModelSchemaStr, preprocessPipelineModelRows,
						sortedLabels, isOutputLogits);
				new TFTableModelClassificationModelDataConverter().save(modelData, out);
			} else {
				TFTableModelRegressionModelData modelData =
					new TFTableModelRegressionModelData(params, featureCols, tfModelRows,
						tfOutputSignatureDef, tfOutputSignatureType,
						preprocessPipelineModelSchemaStr, preprocessPipelineModelRows
					);
				new TFTableModelRegressionModelDataConverter().save(modelData, out);
			}
		}
	}

	public static class ConstructModelMapPartitionFunction extends RichMapPartitionFunction <Row, Row> {

		private final Params params;
		private final String[] featureCols;
		private final String tfOutputSignatureDef;
		private final TypeInformation <?> tfOutputSignatureType;
		private final String preprocessPipelineModelSchemaStr;

		// Indicate the output tensor are logits or not; ignored in regression models.
		private final boolean isOutputLogits;

		private List <Row> preprocessPipelineModelRows;
		private List <Object> sortedLabels;

		public ConstructModelMapPartitionFunction(Params params, String[] featureCols,
												  String tfOutputSignatureDef,
												  TypeInformation <?> tfOutputSignatureType,
												  String preprocessPipelineModelSchemaStr) {
			this(params, featureCols, tfOutputSignatureDef, tfOutputSignatureType,
				preprocessPipelineModelSchemaStr, false);
		}

		public ConstructModelMapPartitionFunction(Params params, String[] featureCols,
												  String tfOutputSignatureDef,
												  TypeInformation <?> tfOutputSignatureType,
												  String preprocessPipelineModelSchemaStr,
												  boolean isOutputLogits) {
			this.params = params;
			this.featureCols = featureCols;
			this.tfOutputSignatureDef = tfOutputSignatureDef;
			this.tfOutputSignatureType = tfOutputSignatureType;
			this.preprocessPipelineModelSchemaStr = preprocessPipelineModelSchemaStr;
			this.isOutputLogits = isOutputLogits;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			RuntimeContext runtimeContext = getRuntimeContext();
			sortedLabels = runtimeContext.hasBroadcastVariable(SORTED_LABELS_BC_NAME)
				? runtimeContext. <List <Object>>getBroadcastVariable(SORTED_LABELS_BC_NAME).get(0)
				: Collections.emptyList();

			preprocessPipelineModelRows = runtimeContext.hasBroadcastVariable(PREPROCESS_PIPELINE_MODEL_BC_NAME)
				? runtimeContext.getBroadcastVariable(PREPROCESS_PIPELINE_MODEL_BC_NAME)
				: Collections.emptyList();
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() != 0) {
				return;
			}
			if (sortedLabels.size() > 0) {
				TFTableModelClassificationModelData modelData =
					new TFTableModelClassificationModelData(params, featureCols, values,
						tfOutputSignatureDef, tfOutputSignatureType,
						preprocessPipelineModelSchemaStr, preprocessPipelineModelRows,
						sortedLabels, isOutputLogits);
				new TFTableModelClassificationModelDataConverter().save(modelData, out);
			} else {
				TFTableModelRegressionModelData modelData =
					new TFTableModelRegressionModelData(params, featureCols, values,
						tfOutputSignatureDef, tfOutputSignatureType,
						preprocessPipelineModelSchemaStr, preprocessPipelineModelRows
					);
				new TFTableModelRegressionModelDataConverter().save(modelData, out);
			}
		}
	}

	static class LabelToIndexMapper extends RichMapFunction <Row, Row> {
		private final int labelColId;
		private Map <Object, Float> labelIndexMap;

		public LabelToIndexMapper(int labelColId) {
			this.labelColId = labelColId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			@SuppressWarnings("unchecked")
			List <Object> sortedLabels =
				(List <Object>) getRuntimeContext().getBroadcastVariable(SORTED_LABELS_BC_NAME)
					.get(0);
			labelIndexMap = new HashMap <>();
			for (int i = 0; i < sortedLabels.size(); i += 1) {
				labelIndexMap.put(sortedLabels.get(i), (float) (i * 1.));
			}
		}

		@Override
		public Row map(Row value) throws Exception {
			Row result = Row.copy(value);
			result.setField(labelColId, labelIndexMap.get(result.getField(labelColId)));
			return result;
		}
	}
}
