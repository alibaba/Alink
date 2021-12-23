package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.AppendIdBatchParams;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Append an id column to BatchOperator. the id can be DENSE or UNIQUE
 *
 * @see DataSetUtils#zipWithIndex
 * @see DataSetUtils#zipWithUniqueId
 */
public final class AppendIdBatchOp extends BatchOperator <AppendIdBatchOp>
	implements AppendIdBatchParams <AppendIdBatchOp> {
	public final static String appendIdColName = "append_id";
	public final static TypeInformation appendIdColType = BasicTypeInfo.LONG_TYPE_INFO;
	private static final long serialVersionUID = 1506253726488454655L;

	public AppendIdBatchOp() {
		super(null);
	}

	public AppendIdBatchOp(Params params) {
		super(params);
	}

	public static Table appendId(DataSet <Row> dataSet, TableSchema schema, Long sessionId) {
		return AppendIdBatchOp.appendId(
			dataSet,
			schema,
			AppendIdBatchOp.appendIdColName,
			AppendType.DENSE,
			sessionId);
	}

	public static Table appendId(
		DataSet <Row> dataSet,
		TableSchema schema,
		String appendIdColName,
		AppendType appendType,
		Long sessionId) {
		String[] rawColNames = schema.getFieldNames();
		TypeInformation[] rawColTypes = schema.getFieldTypes();

		String[] colNames = ArrayUtils.add(rawColNames, appendIdColName);
		TypeInformation[] colTypes = ArrayUtils.add(rawColTypes, appendIdColType);

		DataSet <Row> ret = null;

		switch (appendType) {
			case DENSE:
				ret = DataSetUtils.zipWithIndex(dataSet)
					.map(new TransTupleToRowMapper());
				break;
			case UNIQUE:
				ret = DataSetUtils.zipWithUniqueId(dataSet)
					.map(new TransTupleToRowMapper());
				ret = dataSet.map(new AppendIdMapper());
		}

		return DataSetConversionUtil.toTable(sessionId, ret, colNames, colTypes);
	}

	@Override
	public AppendIdBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(1, inputs);
		this.setOutputTable(appendId(
			inputs[0].getDataSet(),
			inputs[0].getSchema(),
			getIdCol(),
			getAppendType(),
			getMLEnvironmentId()
		));

		return this;
	}

	public static class AppendIdMapper extends RichMapFunction <Row, Row> {
		private static final long serialVersionUID = -1274439106082046078L;
		private long parallelism;
		private long counter;

		@Override
		public void open(Configuration parameters) throws Exception {
			RuntimeContext ctx = getRuntimeContext();
			parallelism = ctx.getNumberOfParallelSubtasks();
			counter = ctx.getIndexOfThisSubtask();
		}

		@Override
		public Row map(Row value) throws Exception {
			Row ret = RowUtil.merge(value, Long.valueOf(counter));
			counter += parallelism;
			return ret;
		}
	}

	public static class TransTupleToRowMapper implements MapFunction <Tuple2 <Long, Row>, Row> {

		private static final long serialVersionUID = 8239750120292573304L;

		@Override
		public Row map(Tuple2 <Long, Row> value) throws Exception {
			Row ret = RowUtil.merge(value.f1, value.f0);
			return ret;
		}
	}
}
