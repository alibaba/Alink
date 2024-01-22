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
 * Full outer join two batch operators.
 */

@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：FullOuterJoin")
public final class FullOuterJoinLocalOp extends BaseSqlApiLocalOp <FullOuterJoinLocalOp>
	implements JoinParams <FullOuterJoinLocalOp> {

	private static final long serialVersionUID = 6002321920184611785L;

	public FullOuterJoinLocalOp() {
		this(new Params());
	}

	public FullOuterJoinLocalOp(String whereClause, String selectClause) {
		this(new Params()
			.set(JoinParams.JOIN_PREDICATE, whereClause)
			.set(JoinParams.SELECT_CLAUSE, selectClause));
	}

	public FullOuterJoinLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		String joinPredicate = getJoinPredicate();
		String selectClause = getSelectClause();
		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor()
			.fullOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause).getOutputTable());
	}
}
