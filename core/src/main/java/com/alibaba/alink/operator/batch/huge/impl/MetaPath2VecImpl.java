package com.alibaba.alink.operator.batch.huge.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.MetaPathWalkBatchOp;
import com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.operator.common.graph.GraphEmbedding;
import com.alibaba.alink.params.nlp.MetaPath2VecParams;

@Internal
public abstract class MetaPath2VecImpl<T extends MetaPath2VecImpl <T>> extends BatchOperator <T>
	implements MetaPath2VecParams <T> {
	private static final long serialVersionUID = -8523576758098834579L;
	ApsCheckpoint checkpoint;

	public MetaPath2VecImpl(Params params) {
		this(params, null);
	}

	public MetaPath2VecImpl(Params params, ApsCheckpoint checkpoint) {
		super(params);
		this.checkpoint = checkpoint;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);
		BatchOperator <?> in1 = inputs[0];
		BatchOperator <?> in2 = inputs[1];

		BatchOperator[] transResult = GraphEmbedding.trans2Index(in1, in2, this.getParams());
		BatchOperator vocab = transResult[0];
		BatchOperator indexedGraph = transResult[1];
		BatchOperator indexWithType = transResult[2];

		try {
			BatchOperator metaPathWalk = new MetaPathWalkBatchOp(this.getParams().clone())
				.setSourceCol(GraphEmbedding.SOURCE_COL)
				.setTargetCol(GraphEmbedding.TARGET_COL)
				.setWeightCol(GraphEmbedding.WEIGHT_COL)
				.setVertexCol(GraphEmbedding.NODE_INDEX_COL)
				.setTypeCol(GraphEmbedding.NODE_TYPE_COL)
				.linkFrom(indexedGraph, indexWithType);

			BatchOperator <?> word2vec;

			if (getMode().equals(Mode.METAPATH2VECPP)) {
				word2vec = new LabeledWord2VecImpl <>(this.getParams().clone(), checkpoint)
					.setSelectedCol(RandomWalkBatchOp.PATH_COL_NAME)
					.setVertexCol(GraphEmbedding.NODE_INDEX_COL)
					.setTypeCol(GraphEmbedding.NODE_TYPE_COL)
					.linkFrom(
						metaPathWalk,
						indexWithType.select("CAST(" + GraphEmbedding.NODE_INDEX_COL + " AS VARCHAR) AS "
							+ GraphEmbedding.NODE_INDEX_COL + ", " + GraphEmbedding.NODE_TYPE_COL)
					);
			} else {
				word2vec = new Word2VecImpl<>(this.getParams().clone(), checkpoint)
					.setSelectedCol(RandomWalkBatchOp.PATH_COL_NAME)
					.linkFrom(metaPathWalk);
			}

			this.setOutputTable(new JoinBatchOp("word=" + GraphEmbedding.NODE_INDEX_COL,
				GraphEmbedding.NODE_COL + ",vec")
				.setMLEnvironmentId(getMLEnvironmentId())
				.linkFrom(word2vec
						.select("CAST(word AS BIGINT) AS word, vec")
					, vocab)
				.getOutputTable());

			return (T) this;

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
