package com.alibaba.alink.operator.batch.huge.impl;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.nlp.LabeledWord2VecParams;

import java.util.ArrayList;
import java.util.List;

@InputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.CORPUS),
	@PortSpec(value = PortType.DATA, desc = PortDesc.NODE_TYPE_MAPPING),
	@PortSpec(value = PortType.MODEL, isOptional = true, desc = PortDesc.INIT_MODEL)
})
@OutputPorts(values = @PortSpec(PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@ParamSelectColumnSpec(name = "vertexCol", portIndices = 1, allowedTypeCollections = TypeCollections.STRING_TYPES)
@ParamSelectColumnSpec(name = "typeCol", portIndices = 1)
@Internal
public class LabeledWord2VecImpl<T extends LabeledWord2VecImpl <T>> extends BatchOperator <T>
	implements LabeledWord2VecParams <T> {
	private static final long serialVersionUID = -451708256014323559L;
	ApsCheckpoint checkpoint;

	public LabeledWord2VecImpl(Params params) {
		super(params);
	}

	public LabeledWord2VecImpl(Params params, ApsCheckpoint checkpoint) {
		super(params);
		this.checkpoint = checkpoint;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(2, inputs);

		Word2VecImpl <?> word2Vec;

		if (checkpoint == null) {
			word2Vec = new Word2VecImpl <>(getParams());
		} else {
			word2Vec = new Word2VecImpl <>(getParams(), checkpoint);
		}

		final String vertexColName = getVertexCol();
		final String typeColName = getTypeCol();

		BatchOperator <?> vec = inputs.length > 2 ? inputs[2] : null;

		List <BatchOperator <?>> orderedInputs = new ArrayList <>();
		orderedInputs.add(inputs[0]);
		orderedInputs.add(vec);
		orderedInputs.add(inputs[1].select("`" + vertexColName + "`, `" + typeColName + "`"));

		this.setOutputTable(word2Vec.linkFrom(orderedInputs).getOutputTable());

		return (T) this;
	}
}
