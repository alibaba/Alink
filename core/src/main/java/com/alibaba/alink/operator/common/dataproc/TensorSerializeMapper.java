package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.Mapper;

import java.util.ArrayList;

/**
 * This mapper serializes vector in corresponding field of the input row.
 */
public class TensorSerializeMapper extends Mapper {
	private static final long serialVersionUID = -9127538856735420252L;

	public TensorSerializeMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		ArrayList <String> tensorCols = new ArrayList <>();
		ArrayList <TypeInformation <?>> tensorColTypes = new ArrayList <>();

		String[] names = dataSchema.getFieldNames();
		TypeInformation <?>[] types = dataSchema.getFieldTypes();

		for (int i = 0; i < types.length; i++) {
			if (AlinkTypes.TENSOR.equals(types[i]) ||
				AlinkTypes.BOOL_TENSOR.equals(types[i]) ||
				AlinkTypes.BYTE_TENSOR.equals(types[i]) ||
				AlinkTypes.INT_TENSOR.equals(types[i]) ||
				AlinkTypes.DOUBLE_TENSOR.equals(types[i]) ||
				AlinkTypes.FLOAT_TENSOR.equals(types[i]) ||
				AlinkTypes.LONG_TENSOR.equals(types[i]) ||
				AlinkTypes.STRING_TENSOR.equals(types[i])) {

				tensorCols.add(names[i]);
				tensorColTypes.add(Types.STRING);
			}
		}

		String[] selectedCols = tensorCols.toArray(new String[0]);
		TypeInformation <?>[] selectedColTypes = tensorColTypes.toArray(new TypeInformation <?>[0]);

		return Tuple4.of(selectedCols, selectedCols, selectedColTypes, null);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selection.length(); i++) {
			Tensor <?> tensor = (Tensor <?>) selection.get(i);
			if (null != tensor) {
				result.set(i, TensorUtil.serialize(tensor));
			}
		}
	}
}

