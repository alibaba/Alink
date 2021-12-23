package com.alibaba.alink.common.dl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.tensorflow.CommonUtils.SORTED_LABELS_BC_NAME;

public class EasyTransferUtils {
	static final String TF_OUTPUT_SIGNATURE_DEF_CLASSIFICATION = "probabilities";
	static final String TF_OUTPUT_SIGNATURE_DEF_REGRESSION = "logits";
	static final TypeInformation <?> TF_OUTPUT_SIGNATURE_TYPE = PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO;

	public static String getTfOutputSignatureDef(TaskType taskType) {
		return TaskType.CLASSIFICATION.equals(taskType)
			? TF_OUTPUT_SIGNATURE_DEF_CLASSIFICATION
			: TF_OUTPUT_SIGNATURE_DEF_REGRESSION;
	}

	public static BatchOperator <?> mapLabelToIntIndex(BatchOperator <?> in, String labelCol,
													   DataSet <List <Object>> sortedLabels) {
		TableSchema schema = in.getSchema();
		int labelColId = TableUtil.findColIndexWithAssertAndHint(schema, labelCol);
		DataSet <Row> mappedDataSet = in.getDataSet()
			.map(new LabelToIntIndexMapper(labelColId))
			.withBroadcastSet(sortedLabels, SORTED_LABELS_BC_NAME);
		String[] fieldNames = schema.getFieldNames();
		TypeInformation <?>[] fieldTypes = schema.getFieldTypes();
		fieldTypes[labelColId] = Types.INT;
		TableSchema mappedSchema = new TableSchema(fieldNames, fieldTypes);
		return BatchOperator.fromTable(
			DataSetConversionUtil.toTable(in.getMLEnvironmentId(), mappedDataSet, mappedSchema))
			.setMLEnvironmentId(in.getMLEnvironmentId());
	}

	static class LabelToIntIndexMapper extends RichMapFunction <Row, Row> {
		private final int labelColId;
		private Map <Object, Integer> labelIndexMap;

		public LabelToIntIndexMapper(int labelColId) {
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
				labelIndexMap.put(sortedLabels.get(i), i);
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
