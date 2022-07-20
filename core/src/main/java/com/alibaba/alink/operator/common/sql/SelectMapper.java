package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.sql.SelectParams;

public class SelectMapper extends Mapper {

	private Mapper mapper;

	public SelectMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public void open() {
		if (SelectUtils.isSimpleSelect(params.get(SelectParams.CLAUSE), this.getDataSchema().getFieldNames())) {
			mapper = new SimpleSelectMapper(this.getDataSchema(), this.params);
		} else {
			mapper = new CalciteSelectMapper(this.getDataSchema(), this.params);
		}
		mapper.open();
	}

	@Override
	public void close() {
		if (mapper != null) {
			mapper.close();
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
	}

	@Override
	public Row map(Row row) throws Exception {
		return mapper.map(row);
	}

	@Override
	public void bufferMap(Row bufferRow, int[] bufferSelectedColIndices, int[] bufferResultColIndices)
		throws Exception {
		mapper.bufferMap(bufferRow, bufferSelectedColIndices, bufferResultColIndices);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		if (SelectUtils.isSimpleSelect(params.get(SelectParams.CLAUSE), dataSchema.getFieldNames())) {
			return SimpleSelectMapper.prepareIoSchemaImpl(dataSchema, params);
		} else {
			return CalciteSelectMapper.prepareIoSchemaImpl(dataSchema, params);
		}
	}

}
