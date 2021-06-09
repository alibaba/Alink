package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;

import java.io.Serializable;

public abstract class ApsSerializeData<DT> implements Serializable {

	private static final String[] DATA_COL_NAMES = new String[] {"rec_id", "rec_value"};
	private static final TypeInformation[] DATA_COL_TYPES = new TypeInformation[] {Types.LONG, Types.STRING};
	private static final long serialVersionUID = 4763752716584677480L;

	protected abstract String serilizeDataType(DT value);

	protected abstract DT deserilizeDataType(String str);

	@Deprecated
	public BatchOperator serializeData(DataSet <Tuple2 <Long, DT>> data) {
		return serializeData(data, MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
	}

	public BatchOperator serializeData(DataSet <Tuple2 <Long, DT>> data, Long mlEnvId) {
		if (data == null) {
			return null;
		}
		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				mlEnvId,
				data.map(new MapFunction <Tuple2 <Long, DT>, Row>() {
					private static final long serialVersionUID = -4805010592295388428L;

					@Override
					public Row map(Tuple2 <Long, DT> value) throws Exception {
						return Row.of(value.f0, serilizeDataType(value.f1));
					}
				}).returns(new RowTypeInfo(DATA_COL_TYPES, DATA_COL_NAMES)),
				DATA_COL_NAMES, DATA_COL_TYPES
			)
		).setMLEnvironmentId(mlEnvId);
	}

	public DataSet <Tuple2 <Long, DT>> deserializeData(BatchOperator <?> in, TypeInformation <DT> dtType) {
		return in
			.getDataSet()
			.map(new MapFunction <Row, Tuple2 <Long, DT>>() {
				private static final long serialVersionUID = 3274309405379826277L;

				@Override
				public Tuple2 <Long, DT> map(Row value) throws Exception {
					return Tuple2.of((Long) value.getField(0),
						deserilizeDataType((String) value.getField(1)));
				}
			})
			.returns(new TupleTypeInfo <>(Types.LONG, dtType));
	}
}
