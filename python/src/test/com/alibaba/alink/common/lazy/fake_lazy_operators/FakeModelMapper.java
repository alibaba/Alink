package com.alibaba.alink.common.lazy.fake_lazy_operators;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;

import java.util.List;

public class FakeModelMapper extends ModelMapper {

	public FakeModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		return Tuple4.of(
			dataSchema.getFieldNames(),
			dataSchema.getFieldNames(),
			dataSchema.getFieldTypes(),
			new String[0]
		);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selection.length(); i += 1) {
			result.set(i, selection.get(i));
		}
	}
}
