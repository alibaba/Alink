package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.JoinParams;

/**
 * Right outer join two batch operators.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：RightOuterJoin")
public final class RightOuterJoinLocalOp extends BaseSqlApiLocalOp <RightOuterJoinLocalOp>
	implements JoinParams <RightOuterJoinLocalOp> {

	private static final long serialVersionUID = -9188072782747998516L;

	public RightOuterJoinLocalOp() {
		this(new Params());
	}

	public RightOuterJoinLocalOp(String whereClause, String selectClause) {
		this(new Params()
			.set(JOIN_PREDICATE, whereClause)
			.set(SELECT_CLAUSE, selectClause));
	}

	public RightOuterJoinLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		String joinPredicate = getJoinPredicate();
		String selectClause = getSelectClause();
		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor()
			.rightOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause)
			.getOutputTable());
	}
}
