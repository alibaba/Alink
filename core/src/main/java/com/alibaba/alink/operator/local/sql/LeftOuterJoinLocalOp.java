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
 * Left outer join two batch operators.
 */

@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：LeftOuterJoin")
public final class LeftOuterJoinLocalOp extends BaseSqlApiLocalOp <LeftOuterJoinLocalOp>
	implements JoinParams <LeftOuterJoinLocalOp> {

	private static final long serialVersionUID = -4614107895339207282L;

	public LeftOuterJoinLocalOp() {
		this(new Params());
	}

	public LeftOuterJoinLocalOp(String whereClause) {
		this(whereClause, "*");
	}

	public LeftOuterJoinLocalOp(String whereClause, String selectClause) {
		this(new Params()
			.set(JOIN_PREDICATE, whereClause)
			.set(SELECT_CLAUSE, selectClause));
	}

	public LeftOuterJoinLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		String joinPredicate = getJoinPredicate();
		String selectClause = getSelectClause();
		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor()
			.leftOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause)
			.getOutputTable());
	}
}
