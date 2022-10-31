package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.exceptions.AkColumnNotFoundException;
import com.alibaba.alink.common.mapper.FlatMapperAdapter;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.FlattenMTableMapper;
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

import java.util.Arrays;
import java.util.function.BiFunction;

import static com.alibaba.alink.common.annotation.PortType.DATA;

/**
 * this base class for outlier algorithms.
 */
@InputPorts(values = @PortSpec(DATA))
@OutputPorts(values = @PortSpec(value = DATA, desc = PortDesc.OUTLIER_DETECTION_RESULT))
@ParamSelectColumnSpec(name = "groupCols")
@Internal
public class BaseOutlierBatchOp<T extends BaseOutlierBatchOp <T>> extends MapBatchOp <T>
	implements OutlierParams <T>, HasMaxOutlierRatio <T>, HasMaxOutlierNumPerGroup <T>,
	HasMaxSampleNumPerGroup <T> {

	public BaseOutlierBatchOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(mapperBuilder, params);
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		if (null == getParams().get(OutlierParams.GROUP_COLS)
			&& !getParams().contains(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP)
			&& supportDealWholeData()
		) {

			setOutputTable(dealWholeData(in));

		}else {
			//Step 1 : Grouped the input rows into MTables
			BatchOperator <?> inGrouped = group2MTables(in, getParams());

			//Step 2 : detect the outlier for each MTable
			Mapper mapper = mapperBuilder.apply(
				inGrouped.getSchema(),
				getParams().clone()
					.set(HasInputMTableCol.INPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
					.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, OutlierDetector.TEMP_MTABLE_COL)
					.set(HasDetectLast.DETECT_LAST, false)
			);
			DataSet <Row> resultRows = MapBatchOp.calcResultRows(inGrouped, mapper, getParams());

			//Step 3 : Flatten the MTables to final results
			Table resultTable = flattenMTable(
				resultRows, in.getSchema(), mapper.getOutputSchema(), getParams(), getMLEnvironmentId()
			);

			setOutputTable(resultTable);
		}

		return (T) this;
	}

	protected boolean supportDealWholeData() {return false;}

	protected Table dealWholeData(BatchOperator <?> in) {return null;}

	/**
	 * make data by group into MTable, and split by maxSampleNumPerGroup.
	 */
	public static BatchOperator <?> group2MTables(BatchOperator <?> inputData, Params params) {
		String[] groupCols = params.get(OutlierParams.GROUP_COLS);
		if (null == groupCols) {
			groupCols = new String[0];
		}

		if (params.contains(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP)) {
			int maxSampleNumPerGroup = params.get(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP);
			return group2MTablesWithMaxSampleNum(inputData, groupCols, maxSampleNumPerGroup);
		} else {
			return group2MTablesWithoutMaxSampleNum(inputData, groupCols);
		}
	}

	/**
	 * make data by group into MTable.
	 */
	public static BatchOperator <?> group2MTablesWithoutMaxSampleNum(BatchOperator <?> inputData,
																	 String[] groupCols) {
		String groupByPredicate = groupCols.length == 0 ? "1" : Joiner.on(", ").join(groupCols);
		String selectClause = "MTABLE_AGG(" + Joiner.on(", ").join(inputData.getColNames())
			+ ") AS " + OutlierDetector.TEMP_MTABLE_COL;

		return inputData.groupBy(groupByPredicate, selectClause);
	}

	/**
	 * A modification implementation of groupBy, which enables each group contains less than maxSampleNumPerGroup
	 * elements.
	 **/
	public static BatchOperator <?> group2MTablesWithMaxSampleNum(BatchOperator <?> inputData,
																  String[] groupCols,
																  int maxSampleNumPerGroup) {
		final int[] groupColIndices = TableUtil.findColIndices(inputData.getColNames(), groupCols);

		DataSet <Row> reGroup = inputData
			.shuffle()
			.getDataSet()
			.groupBy(new GroupKeySelector(groupColIndices))
			.reduceGroup(new ReGroupReduce(maxSampleNumPerGroup, TableUtil.schema2SchemaStr(inputData.getSchema())))
			.rebalance();

		String[] outColNames = new String[] {OutlierDetector.TEMP_MTABLE_COL};
		TypeInformation <?>[] outColTypes = new TypeInformation <?>[] {AlinkTypes.M_TABLE};

		return new TableSourceBatchOp(DataSetConversionUtil.toTable(
			inputData.getMLEnvironmentId(),
			reGroup,
			outColNames,
			outColTypes
		));
	}

	/**
	 * split data of one group, each group has maxSampleNumPerGroup data,
	 * and the last two groups of data are equally divided.
	 */
	static class ReGroupReduce implements GroupReduceFunction <Row, Row> {
		private final int maxSampleNumPerGroup;
		private final String tableSchemaStr;

		private ReGroupReduce(int maxSampleNumPerGroup, String tableSchemaStr) {
			this.maxSampleNumPerGroup = maxSampleNumPerGroup;
			this.tableSchemaStr = tableSchemaStr;
		}

		@Override
		public void reduce(Iterable <Row> values, Collector <Row> out) {
			int idx = 0;
			int maxBufferLen = 2 * maxSampleNumPerGroup;
			Row[] buffer = new Row[maxBufferLen];

			for (Row val : values) {
				if (idx < maxBufferLen) {
					buffer[idx] = val;
					idx++;
				} else {
					out.collect(Row.of(
						new MTable(Arrays.copyOfRange(buffer, maxSampleNumPerGroup, maxBufferLen), tableSchemaStr)
					));
					buffer[maxSampleNumPerGroup] = val;
					idx = maxSampleNumPerGroup + 1;
				}
			}

			if (idx < maxSampleNumPerGroup) {
				out.collect(Row.of(
					new MTable(Arrays.copyOfRange(buffer, 0, idx), tableSchemaStr)));
			} else {
				int halfIdx = (int) Math.round(idx / 2.0);
				out.collect(Row.of(
					new MTable(Arrays.copyOfRange(buffer, 0, halfIdx), tableSchemaStr)));
				out.collect(Row.of(
					new MTable(Arrays.copyOfRange(buffer, halfIdx, idx), tableSchemaStr)));
			}
		}
	}

	/**
	 * group key selector.
	 */
	static class GroupKeySelector implements KeySelector <Row, String> {
		private final int[] groupColIndices;

		GroupKeySelector(int[] groupColIndices) {
			this.groupColIndices = groupColIndices;
		}

		@Override
		public String getKey(Row value) throws Exception {
			if (0 == groupColIndices.length) {
				return "1";
			} else {
				StringBuilder keyBuilder = new StringBuilder();
				for (int i = 0; i < groupColIndices.length; i++) {
					if (null == value.getField(i)) {
						throw new AkColumnNotFoundException("There is NULL value in group col!");
					}
					keyBuilder.append(value.getField(groupColIndices[i]));
					keyBuilder.append("\001");
				}
				return keyBuilder.toString();
			}
		}
	}

	public static Table flattenMTable(DataSet <Row> inputRows, TableSchema input_schema,
									  TableSchema mapper_schema, Params params, Long mlEnvironmentId) {
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

		return DataSetConversionUtil.toTable(
			mlEnvironmentId,
			inputRows.flatMap(new FlatMapperAdapter(flattenMTableMapper)),
			flattenMTableMapper.getOutputSchema());
	}

}