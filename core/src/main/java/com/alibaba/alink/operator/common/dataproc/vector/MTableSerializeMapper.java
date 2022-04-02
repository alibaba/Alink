package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.JsonConverter;

import java.util.ArrayList;

/**
 * This mapper serializes MTable in corresponding field of the input row.
 */
public class MTableSerializeMapper extends Mapper {
	private static final long serialVersionUID = -9127538856735420252L;

	public MTableSerializeMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		ArrayList <String> mTableCols = new ArrayList <>();
		ArrayList <TypeInformation <?>> mTableColTypes = new ArrayList <>();

		String[] names = dataSchema.getFieldNames();
		TypeInformation <?>[] types = dataSchema.getFieldTypes();

		for (int i = 0; i < types.length; i++) {
			if (AlinkTypes.M_TABLE.equals(types[i])) {

				mTableCols.add(names[i]);
				mTableColTypes.add(Types.STRING);
			}
		}

		String[] selectedCols = mTableCols.toArray(new String[0]);
		TypeInformation <?>[] selectedColTypes = mTableColTypes.toArray(new TypeInformation <?>[0]);

		return Tuple4.of(selectedCols, selectedCols, selectedColTypes, null);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selection.length(); i++) {
			MTable mTable = (MTable) selection.get(i);
			if (null != mTable) {
				result.set(i, JsonConverter.toJson(mTable));
			}
		}
	}
}

