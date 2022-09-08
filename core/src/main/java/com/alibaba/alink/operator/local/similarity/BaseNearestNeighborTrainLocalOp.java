package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.NearestNeighborDataConverter;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.nlp.HasIdCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

import java.util.List;

/**
 * Build the index the calculating the nearest neighbor of string.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "idCol", portIndices = 0)
@ParamSelectColumnSpec(name = "selectCol")
public abstract class BaseNearestNeighborTrainLocalOp<T extends BaseNearestNeighborTrainLocalOp<T>>
	extends LocalOperator <T> {

	public final static ParamInfo <TrainType> TRAIN_TYPE = ParamInfoFactory
		.createParamInfo("trainType", TrainType.class)
		.setDescription("id colname")
		.setRequired()
		.setAlias(new String[] {"idColName"})
		.build();

	public BaseNearestNeighborTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		String selectedCol = getParams().get(HasSelectedCol.SELECTED_COL);
		String idCol = getParams().get(HasIdCol.ID_COL);
		TypeInformation idType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), idCol);
		getParams().set(NearestNeighborDataConverter.ID_TYPE, FlinkTypeConverter.getTypeString(idType));

		NearestNeighborDataConverter modelData = getParams().get(TRAIN_TYPE).dataConverter.apply(getParams());
		modelData.setIdType(idType);

		List <Row> out = modelData.buildIndex(in.getOutputTable().select(new String[] {idCol, selectedCol}), getParams());

		this.setOutputTable(new MTable(out, modelData.getModelSchema()));

		return (T) this;
	}

}

