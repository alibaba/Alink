package com.alibaba.alink.operator.batch.huge.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.nlp.LabeledWord2VecParams;

import java.util.ArrayList;
import java.util.List;

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
