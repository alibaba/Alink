package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.Mapper;

import java.util.ArrayList;

/**
 * This mapper serializes vector in corresponding field of the input row.
 */
public class VectorSerializeMapper extends Mapper {
	private static final long serialVersionUID = -9127538856735420252L;

	public VectorSerializeMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		ArrayList <String> vectorCols = new ArrayList <>();
		ArrayList <TypeInformation <?>> vectorColTypes = new ArrayList <>();

		String[] names = dataSchema.getFieldNames();
		TypeInformation <?>[] types = dataSchema.getFieldTypes();

		for (int i = 0; i < types.length; i++) {
			if (AlinkTypes.VECTOR.equals(types[i]) ||
				AlinkTypes.DENSE_VECTOR.equals(types[i]) ||
				AlinkTypes.SPARSE_VECTOR.equals(types[i])) {

				vectorCols.add(names[i]);
				vectorColTypes.add(Types.STRING);
			}
		}

		String[] selectedCols = vectorCols.toArray(new String[0]);
		TypeInformation <?>[] selectedColTypes = vectorColTypes.toArray(new TypeInformation <?>[0]);

		return Tuple4.of(selectedCols, selectedCols, selectedColTypes, null);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selection.length(); i++) {
			Vector vector = (Vector) selection.get(i);
			if (null != vector) {
				result.set(i, VectorUtil.serialize(vector));
			}
		}
	}
}

