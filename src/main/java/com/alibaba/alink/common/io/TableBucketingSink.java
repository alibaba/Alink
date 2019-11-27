package com.alibaba.alink.common.io;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.io.TableBucketingSinkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Bucketing sink function.
 */
public class TableBucketingSink extends RichSinkFunction <Row> {

	private final static Logger LOG = LoggerFactory.getLogger(TableBucketingSink.class);

	private Map <Long, Tuple2 <Long, RichOutputFormat>> formats = new HashMap <>(0);

	private TypeInformation[] types;
	private String[] colNames;
	private String tableNamePrefix;
	private BaseDB db;
	private long currentId = 0;
	private long startTime = System.currentTimeMillis();
	private int batchSize;
	private long batchRolloverInterval;

	public TableBucketingSink(String tableName, Params params, TableSchema schema, BaseDB db) {
		this.tableNamePrefix = tableName;
		this.types = schema.getFieldTypes();
		this.colNames = schema.getFieldNames();
		this.db = db;

		this.batchRolloverInterval = params.get(TableBucketingSinkParams.BATCH_ROLLOVER_INTERVAL);
		this.batchSize = params.get(TableBucketingSinkParams.BATCH_SIZE);
		if (batchSize > 0 && batchRolloverInterval < 0L) {
			batchRolloverInterval = Long.MAX_VALUE;
		}
		if (batchSize < 0 && batchRolloverInterval > 0L) {
			batchSize = Integer.MAX_VALUE;
		}
	}

	@Override
	public void invoke(Row row) throws Exception {
		if (batchRolloverInterval < 0L && batchSize < 0) {
			writeByRuler(row);
		} else {
			writeBySizeOrTime(row);
		}
	}

	private void writeByRuler(Row r) throws Exception {
		Long id = (long) (r).getField(0);
		Long nTab = (long) (r).getField(1);

		Row row = new Row(r.getArity() - 2);

		for (int i = 0; i < row.getArity(); ++i) {
			row.setField(i, r.getField(i + 2));
		}
		if (formats.containsKey(id)) {
			formats.get(id).f0++;
			formats.get(id).f1.writeRecord(row);

			if (formats.get(id).f0.equals(nTab)) {
				formats.get(id).f1.close();
				formats.remove(id);
			}
		} else {

			int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
			LOG.info("{} check create table: {}_{}", indexOfThisSubtask, tableNamePrefix, id);
			if (indexOfThisSubtask == 0) {

				if (!db.hasTable(this.tableNamePrefix + "_" + id)) {
					LOG.info("{} check table: {}_{}", indexOfThisSubtask, tableNamePrefix, id);

					db.createTable(tableNamePrefix + "_" + id, new TableSchema(colNames, types),
						new Params().set(TableBucketingSinkParams.IS_OVER_WRITE, false));

					LOG.info("{} create table: {}_{} OK!", indexOfThisSubtask, tableNamePrefix, id);
				} else {
					throw new RuntimeException("table : " + tableNamePrefix + "_" + id
						+ " has already exists, please change your table name.");
				}
				RichOutputFormat format = db.createFormat(
					tableNamePrefix + "_" + id, new TableSchema(this.colNames, this.types));
				RuntimeContext ctx = getRuntimeContext();
				format.setRuntimeContext(ctx);
				format.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

				format.writeRecord(row);
				formats.put(id, Tuple2.of(1L, format));
			} else {
				while (!db.hasTable(this.tableNamePrefix + "_" + id)) {
					LOG.info("Waiting table to be ready ...");
					Thread.sleep(1000);
				}
				RichOutputFormat format = db.createFormat(
					tableNamePrefix + "_" + id, new TableSchema(this.colNames, this.types));
				RuntimeContext ctx = getRuntimeContext();
				format.setRuntimeContext(ctx);
				format.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

				format.writeRecord(row);
				formats.put(id, Tuple2.of(1L, format));
			}
		}
	}

	private void writeBySizeOrTime(Row row) throws Exception {
		if (formats.containsKey(currentId)) {
			formats.get(currentId).f0++;
			formats.get(currentId).f1.writeRecord(row);

			if (formats.get(currentId).f0 >= batchSize
				|| System.currentTimeMillis() - startTime > batchRolloverInterval) {
				formats.get(currentId).f1.close();
				formats.remove(currentId);
				startTime = System.currentTimeMillis();
				currentId++;
			}
		} else {

			int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
			LOG.info("{} check create table: {}_{}", indexOfThisSubtask, tableNamePrefix, currentId);
			if (indexOfThisSubtask == 0) {

				if (!db.hasTable(this.tableNamePrefix + "_" + currentId)) {
					LOG.info("{} create table: {}_{}", indexOfThisSubtask, tableNamePrefix, currentId);
					db.createTable(tableNamePrefix + "_" + currentId, new TableSchema(colNames, types),
						new Params().set(TableBucketingSinkParams.IS_OVER_WRITE, false));

					LOG.info("{} create table: {}_{} OK!", indexOfThisSubtask, tableNamePrefix, currentId);
				}
				RichOutputFormat format = db.createFormat(
					tableNamePrefix + "_" + currentId, new TableSchema(this.colNames, this.types));
				RuntimeContext ctx = getRuntimeContext();
				format.setRuntimeContext(ctx);
				format.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

				format.writeRecord(row);
				formats.put(currentId, Tuple2.of(1L, format));
			} else {
				while (!db.hasTable(this.tableNamePrefix + "_" + currentId)) {
					LOG.info("Waiting table to be ready ...");
					Thread.sleep(1000);
				}
				RichOutputFormat format = db.createFormat(
					tableNamePrefix + "_" + currentId, new TableSchema(this.colNames, this.types));
				RuntimeContext ctx = getRuntimeContext();
				format.setRuntimeContext(ctx);
				format.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

				format.writeRecord(row);
				formats.put(currentId, Tuple2.of(1L, format));
			}
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (batchSize > 0) {
			batchSize = Math.max(1, batchSize / getRuntimeContext().getNumberOfParallelSubtasks());
		}
	}

	@Override
	public void close() throws Exception {
		if (formats.size() > 0) {
			for (Long id : formats.keySet()) {
				formats.get(id).f1.close();
			}
		}
		super.close();
	}
}
