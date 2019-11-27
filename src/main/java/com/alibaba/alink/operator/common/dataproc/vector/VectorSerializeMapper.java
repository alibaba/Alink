package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.VectorTypes;

import java.util.ArrayList;

/**
 * This mapper serializes vector in corresponding field of the input row.
 */
public class VectorSerializeMapper extends Mapper {
	private final boolean needSerialize;
	private final String[] colNames;
	private final TypeInformation[] colTypes;
	private ArrayList <Integer> colIndices = new ArrayList <>();

	public VectorSerializeMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		TypeInformation[] types = dataSchema.getFieldTypes();
		for (int i = 0; i < types.length; i++) {
			if (VectorTypes.VECTOR.equals(types[i]) ||
					VectorTypes.DENSE_VECTOR.equals(types[i]) ||
					VectorTypes.SPARSE_VECTOR.equals(types[i])) {
				colIndices.add(i);
			}
		}
		this.needSerialize = colIndices.size() > 0;
		for (Integer idx : colIndices) {
			types[idx] = Types.STRING;
		}
		this.colNames = dataSchema.getFieldNames();
		this.colTypes = types;
	}

	@Override
	public TableSchema getOutputSchema() {
		return new TableSchema(this.colNames, this.colTypes);
	}

	@Override
	public Row map(Row row) throws Exception {
		if (this.needSerialize) {
			for (Integer idx : colIndices) {
				Vector vector = (Vector) row.getField(idx);
				if (null != vector) {
					row.setField(idx, vector.toString());
				}
			}
		}
		return row;
	}
}

