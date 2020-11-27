package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.recommendation.FlattenKObjectParams;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlattenKObjectMapper extends FlatMapper {
	private static final long serialVersionUID = 5345439790133072507L;
	private OutputColsHelper outputColsHelper;
	private int selectIdx;
	private String[] outputColNames;
	private Type[] outputColJavaTypes;

	public FlattenKObjectMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		selectIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema, params.get(FlattenKObjectParams.SELECTED_COL));

		outputColNames = params.get(FlattenKObjectParams.OUTPUT_COLS);
		TypeInformation <?>[] outputColTypes = new TypeInformation <?>[outputColNames.length];
		outputColJavaTypes = new Type[outputColNames.length];

		if (params.contains(FlattenKObjectParams.OUTPUT_COL_TYPES)) {
			String[] typesStr = params.get(FlattenKObjectParams.OUTPUT_COL_TYPES);

			Preconditions.checkState(
				outputColNames.length == typesStr.length,
				"the length of output column names should be equal to the length of output column types.");

			for (int i = 0; i < outputColTypes.length; ++i) {
				outputColTypes[i] = FlinkTypeConverter.getFlinkType(typesStr[i].toUpperCase());
				outputColJavaTypes[i] = outputColTypes[i].getTypeClass();
			}

		} else {
			Arrays.fill(outputColTypes, Types.STRING);
			Arrays.fill(outputColJavaTypes, Types.STRING.getTypeClass());
		}

		outputColsHelper = new OutputColsHelper(
			dataSchema,
			outputColNames,
			outputColTypes,
			params.get(FlattenKObjectParams.RESERVED_COLS));
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	@Override
	public void flatMap(Row row, Collector <Row> output) {
		Object[] result = new Object[outputColNames.length];
		String s = (String) row.getField(selectIdx);
		if (null == s) {
			output.collect(outputColsHelper.getResultRow(row, Row.of(result)));
			return;
		}

		Map <String, List <Object>> deserialized =
			KObjectUtil.deserializeKObject(
				(String) row.getField(selectIdx),
				outputColNames,
				outputColJavaTypes
			);

		if (deserialized == null || deserialized.isEmpty()) {
			output.collect(outputColsHelper.getResultRow(row, Row.of(result)));
			return;
		}

		List <Object> firstUnEmpty = null;
		for (Map.Entry <String, List <Object>> entry : deserialized.entrySet()) {
			firstUnEmpty = entry.getValue();
			if (firstUnEmpty != null) {
				break;
			}
		}

		if (firstUnEmpty == null) {
			output.collect(outputColsHelper.getResultRow(row, Row.of(result)));
			return;
		}

		for (int i = 0; i < firstUnEmpty.size(); ++i) {
			for (int j = 0; j < outputColNames.length; ++j) {
				List <Object> element = deserialized.get(outputColNames[j]);
				result[j] = element == null ? null : element.get(i);
			}
			output.collect(outputColsHelper.getResultRow(row, Row.of(result)));
		}
	}
}
