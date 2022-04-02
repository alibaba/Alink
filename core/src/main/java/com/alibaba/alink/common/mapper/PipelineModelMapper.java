package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.pipeline.ModelExporterUtils;

import java.util.Arrays;
import java.util.List;

public class PipelineModelMapper extends ComboModelMapper {

	public static final String SPLITER_COL_NAME = "dynamic_pipeline_model_schema_spliter";
	public static final TypeInformation <?> SPLITER_COL_TYPE = Types.DOUBLE;
	static final String OUT_COL_PREFIX = "extended_";

	public static ParamInfo <String[]> PIPELINE_TRANSFORM_OUT_COL_NAMES = ParamInfoFactory
		.createParamInfo("__pipeline_transform_out_col_names__", String[].class)
		.build();

	public static ParamInfo <String[]> PIPELINE_TRANSFORM_OUT_COL_TYPES = ParamInfoFactory
		.createParamInfo("__pipeline_transform_out_col_types__", String[].class)
		.build();

	public PipelineModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		if (isExtendModel(params)) {
			Tuple2 <String[], TypeInformation <?>[]> tuple2 = getExtendModelSchema(modelSchema);
			return Tuple4.of(this.getDataSchema().getFieldNames(), tuple2.f0, tuple2.f1, new String[] {});
		} else {
			return Tuple4.of(this.getDataSchema().getFieldNames(),
				params.get(PIPELINE_TRANSFORM_OUT_COL_NAMES),
				FlinkTypeConverter.getFlinkType(params.get(PIPELINE_TRANSFORM_OUT_COL_TYPES)),
				new String[] {});
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		TableSchema modelSchema = this.getModelSchema();
		if (isExtendModel(this.params)) {
			String[] modelColNames = this.getModelSchema().getFieldNames();
			TypeInformation <?>[] modelColTypes = this.getModelSchema().getFieldTypes();
			Tuple2 <String[], TypeInformation <?>[]> tuple2 = getExtendModelSchema(this.getModelSchema());

			int fromIdx = 0;
			int toIdx = modelColNames.length - 1 - tuple2.f0.length;

			String[] colNames = Arrays.copyOfRange(modelColNames, fromIdx, toIdx);
			TypeInformation <?>[] colTypes = Arrays.copyOfRange(modelColTypes, fromIdx, toIdx);
			modelSchema = new TableSchema(colNames, colTypes);
		}
		mapperList = ModelExporterUtils.loadMapperListFromStages(modelRows, modelSchema, getDataSchema());
		if (!getOutputSchema().equals(mapperList.getOutTableSchema())) {
			throw new RuntimeException("Load pipeline model failed.");
		}
	}

	private boolean isExtendModel(Params params) {
		return !params.contains(PIPELINE_TRANSFORM_OUT_COL_NAMES) ||
			!params.contains(PIPELINE_TRANSFORM_OUT_COL_TYPES);
	}

	public static class ExtendPipelineModelRow implements MapFunction <Row, Row> {
		private static final long serialVersionUID = 4352180823329796206L;
		private final int extendLen;

		public ExtendPipelineModelRow(int extendLen) {
			this.extendLen = extendLen;
		}

		@Override
		public Row map(Row value) {
			Row out = new Row(value.getArity() + extendLen);
			for (int i = 0; i < value.getArity(); i++) {
				out.setField(i, value.getField(i));
			}
			return out;
		}
	}

	public static TableSchema getExtendModelSchema(TableSchema modelSchema,
												   String[] extendedColNames,
												   TypeInformation <?>[] extendedColTypes) {
		String[] originCols = modelSchema.getFieldNames();
		TypeInformation <?>[] originTypes = modelSchema.getFieldTypes();

		String[] newCols = new String[originCols.length + 1 + extendedColNames.length];
		TypeInformation <?>[] newTypes = new TypeInformation[newCols.length];

		System.arraycopy(originCols, 0, newCols, 0, originCols.length);
		System.arraycopy(originTypes, 0, newTypes, 0, originTypes.length);

		newCols[originCols.length] = PipelineModelMapper.SPLITER_COL_NAME;
		newTypes[originCols.length] = PipelineModelMapper.SPLITER_COL_TYPE;

		int selectColStartIndex = originCols.length + 1;
		for (int i = 0; i < extendedColNames.length; i++) {
			newCols[selectColStartIndex + i] = OUT_COL_PREFIX + extendedColNames[i];
		}
		System.arraycopy(extendedColTypes, 0, newTypes, originCols.length + 1, extendedColTypes.length);

		return new TableSchema(newCols, newTypes);
	}

	public static Tuple2 <String[], TypeInformation <?>[]> getExtendModelSchema(
		TableSchema extendedModelSchema) {

		String[] modelCols = extendedModelSchema.getFieldNames();
		TypeInformation <?>[] modelTypes = extendedModelSchema.getFieldTypes();

		int splitColIndex = TableUtil.findColIndexWithAssert(
			extendedModelSchema, PipelineModelMapper.SPLITER_COL_NAME
		);

		Preconditions.checkArgument(splitColIndex >= 0, "Scorecard model schema error!");

		int selectColStartIndex = splitColIndex + 1;

		String[] extendedColNames = new String[modelCols.length - selectColStartIndex];
		TypeInformation <?>[] extendedColTypes = new TypeInformation[extendedColNames.length];

		for (int i = 0; i < extendedColNames.length; i++) {
			extendedColNames[i] = modelCols[selectColStartIndex + i].substring(9);
			extendedColTypes[i] = modelTypes[selectColStartIndex + i];
		}

		return Tuple2.of(extendedColNames, extendedColTypes);
	}
}
