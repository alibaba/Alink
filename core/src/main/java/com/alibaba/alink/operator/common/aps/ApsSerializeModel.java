package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;

import java.io.Serializable;

public abstract class ApsSerializeModel<MT> implements Serializable {
	private final static String[] MODEL_COL_NAMES = new String[] {"model_id", "model_val"};
	private final static TypeInformation[] MODEL_COL_TYPES = new TypeInformation[] {Types.LONG, Types.STRING};
	private static final long serialVersionUID = -902097340305955579L;

	protected abstract String serilizeModelType(MT value);

	protected abstract MT deserilizeModelType(String str);

	@Deprecated
	public BatchOperator serializeModel(DataSet <Tuple2 <Long, MT>> model) {
		return serializeModel(model, MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
	}

	public BatchOperator serializeModel(DataSet <Tuple2 <Long, MT>> model, Long mlEnvId) {
		if (model == null) {
			return null;
		}

		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				mlEnvId,
				model.map(new MapFunction <Tuple2 <Long, MT>, Row>() {
					private static final long serialVersionUID = 8987110643749946565L;

					@Override
					public Row map(Tuple2 <Long, MT> value) throws Exception {
						return Row.of(value.f0, serilizeModelType(value.f1));
					}
				})
					.returns(new RowTypeInfo(MODEL_COL_TYPES, MODEL_COL_NAMES)),
				MODEL_COL_NAMES,
				MODEL_COL_TYPES)
		).setMLEnvironmentId(mlEnvId);
	}

	public DataSet <Tuple2 <Long, MT>> deserilizeModel(
		BatchOperator <?> model, TypeInformation <Tuple2 <Long, MT>> mType) {

		return model
			.getDataSet()
			.map(new MapFunction <Row, Tuple2 <Long, MT>>() {
				private static final long serialVersionUID = -700595814780721183L;

				@Override
				public Tuple2 <Long, MT> map(Row row) throws Exception {
					return Tuple2.of((Long) row.getField(0), deserilizeModelType((String) row.getField(1)));
				}
			})
			.returns(mType)
			.partitionCustom(new Partitioner <Long>() {
				private static final long serialVersionUID = 3299173840265739458L;

				@Override
				public int partition(Long key, int numPartitions) {
					return Math.abs(key.intValue()) % numPartitions;
				}
			}, 0);
	}
}
