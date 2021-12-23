package com.alibaba.alink.operator.batch.huge.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.Node2VecWalkBatchOp;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.operator.common.graph.GraphEmbedding;
import com.alibaba.alink.params.nlp.Node2VecParams;

@Internal
public abstract class Node2VecImpl<T extends Node2VecImpl <T>> extends BatchOperator <T>
	implements Node2VecParams <T> {
	private static final long serialVersionUID = -2095797659216791404L;
	ApsCheckpoint checkpoint;

	public Node2VecImpl(Params params) {
		super(params);
	}

	public Node2VecImpl(Params params, ApsCheckpoint checkpoint) {
		super(params);
		this.checkpoint = checkpoint;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		BatchOperator[] transResult = GraphEmbedding.trans2Index(in, null, this.getParams());
		BatchOperator vocab = transResult[0];
		BatchOperator indexedGraph = transResult[1];

		try {
			this.setOutputTable(new JoinBatchOp("word=" + GraphEmbedding.NODE_INDEX_COL,
				GraphEmbedding.NODE_COL + ",vec")
				.setMLEnvironmentId(getMLEnvironmentId())
				.linkFrom(indexedGraph
						.link(new Node2VecWalkBatchOp(this.getParams().clone())
							.setSourceCol(GraphEmbedding.SOURCE_COL)
							.setTargetCol(GraphEmbedding.TARGET_COL)
							.setWeightCol(GraphEmbedding.WEIGHT_COL))
						.link(new Word2VecImpl <>(this.getParams().clone(), checkpoint)
							.setSelectedCol(Node2VecWalkBatchOp.PATH_COL_NAME))
						.select("CAST(word AS BIGINT) AS word, vec")
					, vocab)
				.getOutputTable());

			return (T) this;

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}
