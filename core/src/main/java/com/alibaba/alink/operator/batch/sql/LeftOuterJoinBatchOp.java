package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.JoinParams;

/**
 * Left outer join two batch operators.
 */

@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：LeftOuterJoin")
@NameEn("SQL LeftOuterJoin Operation")
public final class LeftOuterJoinBatchOp extends BaseSqlApiBatchOp <LeftOuterJoinBatchOp>
	implements JoinParams <LeftOuterJoinBatchOp> {

	private static final long serialVersionUID = -4614107895339207282L;

	public LeftOuterJoinBatchOp() {
		this(new Params());
	}

	public LeftOuterJoinBatchOp(String whereClause) {
		this(whereClause, "*");
	}

	public LeftOuterJoinBatchOp(String whereClause, String selectClause) {
		this(new Params()
			.set(JOIN_PREDICATE, whereClause)
			.set(SELECT_CLAUSE, selectClause));
	}

	public LeftOuterJoinBatchOp(Params params) {
		super(params);
	}

	@Override
	public LeftOuterJoinBatchOp linkFrom(BatchOperator <?>... inputs) {
		String joinPredicate = getJoinPredicate();
		String selectClause = getSelectClause();
		this.setOutputTable(
			BatchSqlOperators.leftOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause).getOutputTable());
		return this;
	}
}
