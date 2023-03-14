package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.FlattenMTableMapper;
import com.alibaba.alink.operator.common.outlier.OutlierDetector;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.FlattenMTableParams;
import com.alibaba.alink.params.outlier.HasDetectLast;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasMaxOutlierNumPerGroup;
import com.alibaba.alink.params.outlier.HasMaxOutlierRatio;
import com.alibaba.alink.params.outlier.HasMaxSampleNumPerGroup;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.OutlierDetectorParams;
import com.alibaba.alink.params.outlier.OutlierParams;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;
import java.util.function.BiFunction;

import static com.alibaba.alink.common.annotation.PortType.DATA;

/**
 * this base class for outlier algorithms.
 */
@InputPorts(values = @PortSpec(DATA))
@OutputPorts(values = @PortSpec(value = DATA, desc = PortDesc.OUTLIER_DETECTION_RESULT))
@ParamSelectColumnSpec(name = "groupCols")
@Internal
public class BaseOutlierLocalOp<T extends BaseOutlierLocalOp <T>> extends MapLocalOp <T>
	implements OutlierParams <T>, HasMaxOutlierRatio <T>, HasMaxOutlierNumPerGroup <T>,
	HasMaxSampleNumPerGroup <T> {

	public BaseOutlierLocalOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(mapperBuilder, params);
	}

	@Override
	public T linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		if (null == getParams().get(OutlierParams.GROUP_COLS)
			&& !getParams().contains(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP)
			&& supportDealWholeData()
		) {

			setOutputTable(dealWholeData(in));

		} else {
			//Step 1 : Grouped the input rows into MTables
			LocalOperator <?> inGrouped = group2MTables(in, getParams());

			//Step 2 : detect the outlier for each MTable
			Mapper mapper = mapperBuilder.apply(
				inGrouped.getSchema(),
				getParams().clone()
					.set(HasInputMTableCol.INPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
					.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
					.set(HasDetectLast.DETECT_LAST, false)
			);
			List <Row> resultRows = MapLocalOp.execMapper(inGrouped, mapper, getParams());

			//Step 3 : Flatten the MTables to final results
			MTable resultTable = flattenMTable(resultRows, in.getSchema(), mapper.getOutputSchema(), getParams());

			setOutputTable(resultTable);
		}

		return (T) this;
	}

	protected boolean supportDealWholeData() {return false;}

	protected MTable dealWholeData(LocalOperator <?> in) {return null;}

	public static LocalOperator <?> group2MTables(LocalOperator <?> in, Params params) {
		RowCollector rowCollector = new RowCollector();

		String[] groupCols = params.get(OutlierParams.GROUP_COLS);
		if (null == groupCols || groupCols.length == 0) {
			subGroupWithSize(in.getOutputTable(), rowCollector, params);
		} else {
			String groupByPredicate = Joiner.on(", ").join(groupCols);
			String selectClause = "MTABLE_AGG(" + Joiner.on(", ").join(in.getColNames())
				+ ") AS " + OutlierDetector.TEMP_MTABLE_COL;

			for (Row r : in.groupBy(groupByPredicate, selectClause).getOutputTable().getRows()) {
				subGroupWithSize((MTable) r.getField(0), rowCollector, params);
			}
		}

		return new TableSourceLocalOp(new MTable(rowCollector.getRows(), new TableSchema(
			new String[] {OutlierDetector.TEMP_MTABLE_COL}, new TypeInformation <?>[] {AlinkTypes.M_TABLE}))
		);
	}

	private static void subGroupWithSize(MTable mt, RowCollector rowCollector, Params params) {
		int n = mt.getNumRow();
		int m = n;
		if (params.contains(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP)) {
			m = params.get(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP);
		}
		int K = (n + m - 1) / m;
		for (int k = 0; k < K; k++) {
			rowCollector.collect(Row.of(mt.subTable(k * m, Math.min(k * m + m, n))));
		}
	}

	public static MTable flattenMTable(List <Row> inputRows, TableSchema input_schema,
									   TableSchema mapper_schema, Params params) {
		String[] mtableCols = ArrayUtils.add(input_schema.getFieldNames(),
			params.get(OutlierDetectorParams.PREDICTION_COL));
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

		RowCollector rowCollector = new RowCollector();
		for (Row row : inputRows) {
			flattenMTableMapper.flatMap(row, rowCollector);
		}

		return new MTable(rowCollector.getRows(), flattenMTableMapper.getOutputSchema());

		//return DataSetConversionUtil.toTable(
		//	mlEnvironmentId,
		//	inputRows.flatMap(new FlatMapperAdapter(flattenMTableMapper)),
		//	flattenMTableMapper.getOutputSchema());
	}

}