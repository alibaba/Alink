package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.sql.SelectParams;

public class SimpleSelectMapper extends Mapper {

	public SimpleSelectMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selection.length(); i++) {
			result.set(i, selection.get(i));
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		return prepareIoSchemaImpl(dataSchema, params);
	}

	static Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchemaImpl(TableSchema dataSchema,
																							Params params) {
		String clause = params.get(SelectParams.CLAUSE);
		Tuple2 <String[], String[]> sTuple = SelectUtils.splitAndTrim(clause, dataSchema.getFieldNames());
		TypeInformation <?>[] sTypes = TableUtil.findColTypesWithAssert(dataSchema, sTuple.f0);
		return Tuple4.of(
			sTuple.f0,
			sTuple.f1,
			sTypes,
			new String[] {}
		);
	}

	//over write output schema, output cols order by clause, otherwise it will order by input schema,
	@Override
	public TableSchema getOutputSchema() {
		String clause = params.get(SelectParams.CLAUSE);
		Tuple2 <String[], String[]> sTuple = SelectUtils.splitAndTrim(clause,
			this.getDataSchema().getFieldNames());
		TypeInformation <?>[] sTypes = TableUtil.findColTypesWithAssert(this.getDataSchema(), sTuple.f0);
		return new TableSchema(sTuple.f1, sTypes);
	}
}
