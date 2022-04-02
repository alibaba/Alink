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
 * Right outer join two batch operators.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：RightOuterJoin")
public final class RightOuterJoinBatchOp extends BaseSqlApiBatchOp <RightOuterJoinBatchOp>
	implements JoinParams <RightOuterJoinBatchOp> {

	private static final long serialVersionUID = -9188072782747998516L;

	public RightOuterJoinBatchOp() {
		this(new Params());
	}

	public RightOuterJoinBatchOp(String whereClause, String selectClause) {
		this(new Params()
			.set(JOIN_PREDICATE, whereClause)
			.set(SELECT_CLAUSE, selectClause));
	}

	public RightOuterJoinBatchOp(Params params) {
		super(params);
	}

	@Override
	public RightOuterJoinBatchOp linkFrom(BatchOperator <?>... inputs) {
		String joinPredicate = getJoinPredicate();
		String selectClause = getSelectClause();
		this.setOutputTable(BatchSqlOperators.rightOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause)
			.getOutputTable());
		return this;
	}
}
