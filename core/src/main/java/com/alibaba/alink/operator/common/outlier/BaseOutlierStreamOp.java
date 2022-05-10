package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.mapper.FlatMapperAdapter;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.FlattenMTableMapper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.FlattenMTableParams;
import com.alibaba.alink.params.feature.featuregenerator.HasPrecedingRows;
import com.alibaba.alink.params.feature.featuregenerator.HasPrecedingTime;
import com.alibaba.alink.params.feature.featuregenerator.OverCountWindowParams;
import com.alibaba.alink.params.outlier.HasDetectLast;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.OutlierDetectorParams;
import com.alibaba.alink.params.outlier.OutlierParams;
import com.alibaba.alink.params.shared.colname.HasTimeColDefaultAsNull;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.ArrayUtils;

import java.util.function.BiFunction;

@Internal
public class BaseOutlierStreamOp<T extends BaseOutlierStreamOp <T>> extends MapStreamOp <T>
	implements OutlierParams <T>, HasPrecedingRows <T>, HasPrecedingTime <T>, HasTimeColDefaultAsNull <T> {

	public BaseOutlierStreamOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(mapperBuilder, params);
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		try {
			//Step 1 : Grouped the input rows into MTables
			StreamOperator <?> in_grouped = group2MTables(in, getParams());

			//Step 2 : detect the outlier for each MTable
			Mapper mapper = this.mapperBuilder.apply(in_grouped.getSchema(),
				getParams().clone()
					.set(HasInputMTableCol.INPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
					.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
					.set(HasDetectLast.DETECT_LAST, true)
			);
			DataStream <Row> resultRows = MapStreamOp.calcResultRows(in_grouped, mapper, getParams());

			//Step 3 : Flatten the MTables to final results
			Table resultTable = flattenMTable(
				resultRows, in.getSchema(), mapper.getOutputSchema(), getParams(), getMLEnvironmentId()
			);

			this.setOutputTable(resultTable);

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static StreamOperator <?> group2MTables(StreamOperator <?> input_data, Params params) {
		Params cur_params = new Params();
		cur_params
			.set(OverCountWindowParams.TIME_COL, params.get(HasTimeColDefaultAsNull.TIME_COL))
			.set(OverCountWindowParams.PRECEDING_ROWS, params.get(HasPrecedingRows.PRECEDING_ROWS))
			.set(OverCountWindowParams.CLAUSE,
				"MTABLE_AGG(" + Joiner.on(", ").join(input_data.getColNames())
					+ ") AS " + OutlierDetector.TEMP_MTABLE_COL
			);
		if (params.contains(OutlierParams.GROUP_COLS)) {
			cur_params.set(OverCountWindowParams.GROUP_COLS, params.get(OutlierParams.GROUP_COLS));
		}

		return input_data.link(
			new OverCountWindowStreamOp(cur_params)
				.setMLEnvironmentId(input_data.getMLEnvironmentId()));
	}

	public static Table flattenMTable(DataStream <Row> inputRows, TableSchema input_schema,
									  TableSchema mapper_schema, Params params, Long mlEnvironmentId) {
		String[] mtableCols = ArrayUtils.add(input_schema.getFieldNames(), params.get(OutlierDetectorParams.PREDICTION_COL));
		TypeInformation <?>[] mtableTypes = ArrayUtils.add(input_schema.getFieldTypes(), AlinkTypes.BOOLEAN);
		if (params.contains(OutlierDetectorParams.PREDICTION_DETAIL_COL)) {
			mtableCols = ArrayUtils.add(mtableCols, params.get(OutlierDetectorParams.PREDICTION_DETAIL_COL));
			mtableTypes = ArrayUtils.add(mtableTypes, AlinkTypes.STRING);
		}
		String schemaStr = TableUtil.schema2SchemaStr(new TableSchema(mtableCols, mtableTypes));

		FlattenMTableMapper flattenMTableMapper = new FlattenMTableMapper(
			mapper_schema,
			new Params()
				.set(FlattenMTableParams.SELECTED_COL, OutlierDetector.TEMP_MTABLE_COL)
				.set(FlattenMTableParams.SCHEMA_STR, schemaStr)
				.set(FlattenMTableParams.RESERVED_COLS, new String[0])
		);

		return DataStreamConversionUtil.toTable(
			mlEnvironmentId,
			inputRows.flatMap(new FlatMapperAdapter(flattenMTableMapper)),
			flattenMTableMapper.getOutputSchema());
	}

}