package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.NearestNeighborDataConverter;
import com.alibaba.alink.params.nlp.HasIdCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Build the index the calculating the nearest neighbor of string.
 */
public abstract class BaseNearestNeighborTrainBatchOp<T extends BaseNearestNeighborTrainBatchOp<T>>
	extends BatchOperator <T> {
	private static final long serialVersionUID = 8494407021938399142L;

	public final static ParamInfo <TrainType> TRAIN_TYPE = ParamInfoFactory
		.createParamInfo("trainType", TrainType.class)
		.setDescription("id colname")
		.setRequired()
		.setAlias(new String[] {"idColName"})
		.build();

	public BaseNearestNeighborTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String selectedCol = getParams().get(HasSelectedCol.SELECTED_COL);
		String idCol = getParams().get(HasIdCol.ID_COL);
		TypeInformation idType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), idCol);
		getParams().set(NearestNeighborDataConverter.ID_TYPE, FlinkTypeConverter.getTypeString(idType));

		NearestNeighborDataConverter modelData = getParams().get(TRAIN_TYPE).dataConverter.apply(getParams());
		modelData.setIdType(idType);

		DataSet <Row> out = modelData.buildIndex(in.select(new String[] {idCol, selectedCol}), getParams());

		this.setOutput(out, modelData.getModelSchema());

		return (T) this;
	}

}

