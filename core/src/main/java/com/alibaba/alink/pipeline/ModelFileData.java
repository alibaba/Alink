package com.alibaba.alink.pipeline;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;

import java.util.ArrayList;

class ModelFileData {
	public final ModelPipeFileData modelPipeFileData;
	public final long localId;
	public final int offset;
	public final int[] localSchemaIndices;
	public final TableSchema schema;

	public ModelFileData(ModelPipeFileData modelPipeFileData, long localId, int offset, int[] localSchemaIndices,
						 TableSchema schema) {
		this.modelPipeFileData = modelPipeFileData;
		this.localId = localId;
		this.offset = offset;
		this.localSchemaIndices = localSchemaIndices;
		this.schema = schema;
	}

	public BatchOperator <?> getBatchData(long mlEnvironmentId) {
		final long cur_localId = this.localId;
		final int cur_offset = this.offset;
		final int[] cur_localSchemaIndices = this.localSchemaIndices;

		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				mlEnvironmentId,
				this.modelPipeFileData.getBatchData()
					.setMLEnvironmentId(mlEnvironmentId)
					.getDataSet()
					.filter(new FilterFunction <Row>() {
						private static final long serialVersionUID = 355683133177055891L;

						@Override
						public boolean filter(Row value) {
							return value.getField(0).equals(cur_localId);
						}
					})
					.map(new MapFunction <Row, Row>() {
						private static final long serialVersionUID = -4286266312978550037L;

						@Override
						public Row map(Row value) throws Exception {
							Row ret = new Row(cur_localSchemaIndices.length);

							for (int i = 0; i < cur_localSchemaIndices.length; ++i) {
								ret.setField(i, value.getField(cur_localSchemaIndices[i] + cur_offset));
							}
							return ret;
						}
					})
					.returns(new RowTypeInfo(schema.getFieldTypes())),
				schema
			)
		).setMLEnvironmentId(mlEnvironmentId);

	}

	public LocalOperator <?> getLocalData() {
		LocalOperator <?> op = this.modelPipeFileData.getLocalData();
		String firstColName = op.getColNames()[0];

		MTable relatedRows = op.filter(firstColName + "=" + this.localId)
			.getOutputTable();

		ArrayList <Row> list = new ArrayList <>();
		for (Row value : relatedRows.getRows()) {
			Row ret = new Row(localSchemaIndices.length);
			for (int i = 0; i < localSchemaIndices.length; ++i) {
				ret.setField(i, value.getField(localSchemaIndices[i] + offset));
			}
			list.add(ret);
		}

		return new MemSourceLocalOp(list, schema);
	}

}
