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
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.operator.common.graph.GraphEmbedding;
import com.alibaba.alink.params.nlp.DeepWalkParams;

@InputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.GRAPH))
@OutputPorts(values = @PortSpec(PortType.MODEL))
@ParamSelectColumnSpec(name = "sourceCol")
@ParamSelectColumnSpec(name = "targetCol")
@ParamSelectColumnSpec(name = "weightCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@Internal
public abstract class DeepWalkImpl<T extends DeepWalkImpl <T>> extends BatchOperator <T>
	implements DeepWalkParams <T> {
	private static final long serialVersionUID = -2985084381324085540L;
	ApsCheckpoint checkpoint;

	public DeepWalkImpl(Params params) {
		this(params, null);
	}

	public DeepWalkImpl(Params params, ApsCheckpoint checkpoint) {
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
						.link(new RandomWalkBatchOp(this.getParams().clone())
							.setSourceCol(GraphEmbedding.SOURCE_COL)
							.setTargetCol(GraphEmbedding.TARGET_COL)
							.setWeightCol(GraphEmbedding.WEIGHT_COL))
						.link(new Word2VecImpl <>(this.getParams().clone(), checkpoint)
							.setSelectedCol(RandomWalkBatchOp.PATH_COL_NAME))
						.select("CAST(word AS BIGINT) AS word, vec")
					, vocab)
				.getOutputTable());

			return (T) this;

		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException(ex.getMessage(),ex);
		}
	}
}
