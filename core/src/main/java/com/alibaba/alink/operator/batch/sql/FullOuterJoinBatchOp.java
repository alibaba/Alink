package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.sql.BatchSqlOperators;
import com.alibaba.alink.params.sql.JoinParams;

/**
 * Full outer join two batch operators.
 */

@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：FullOuterJoin")
public final class FullOuterJoinBatchOp extends BaseSqlApiBatchOp <FullOuterJoinBatchOp>
	implements JoinParams <FullOuterJoinBatchOp> {

	private static final long serialVersionUID = 6002321920184611785L;

	public FullOuterJoinBatchOp() {
		this(new Params());
	}

	public FullOuterJoinBatchOp(String whereClause, String selectClause) {
		this(new Params()
			.set(JoinParams.JOIN_PREDICATE, whereClause)
			.set(JoinParams.SELECT_CLAUSE, selectClause));
	}

	public FullOuterJoinBatchOp(Params params) {
		super(params);
	}

	@Override
	public FullOuterJoinBatchOp linkFrom(BatchOperator <?>... inputs) {
		String joinPredicate = getJoinPredicate();
		String selectClause = getSelectClause();
		this.setOutputTable(
			BatchSqlOperators.fullOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause).getOutputTable());
		return this;
	}
}
