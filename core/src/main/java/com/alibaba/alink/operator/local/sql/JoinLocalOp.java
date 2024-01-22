package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.JoinParams;

/**
 * Join two batch operators.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：Join")
public final class JoinLocalOp extends BaseSqlApiLocalOp <JoinLocalOp>
	implements JoinParams <JoinLocalOp> {

	private static final long serialVersionUID = -5284150849586086589L;

	public JoinLocalOp() {
		this(new Params());
	}

	public JoinLocalOp(String joinPredicate) {
		this(joinPredicate, "*");
	}

	public JoinLocalOp(String joinPredicate, String selectClause) {
		this(new Params()
			.set(JOIN_PREDICATE, joinPredicate)
			.set(SELECT_CLAUSE, selectClause));
	}

	public JoinLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		String selectClause = "*";
		if (this.getParams().contains(JoinParams.SELECT_CLAUSE)) {
			selectClause = this.getParams().get(JoinParams.SELECT_CLAUSE);
		}
		String joidPredicate = this.getParams().get(JoinParams.JOIN_PREDICATE);

		LocalOperator <?> outputOp;
		switch (getType()) {
			case JOIN:
				outputOp = LocalMLEnvironment.getInstance().getSqlExecutor().join(inputs[0], inputs[1], joidPredicate,
					selectClause);
				break;
			case LEFTOUTERJOIN:
				outputOp = LocalMLEnvironment.getInstance().getSqlExecutor().leftOuterJoin(inputs[0], inputs[1],
					joidPredicate,
					selectClause);
				break;
			case RIGHTOUTERJOIN:
				outputOp = LocalMLEnvironment.getInstance().getSqlExecutor().rightOuterJoin(inputs[0], inputs[1],
					joidPredicate,
					selectClause);
				break;
			case FULLOUTERJOIN:
				outputOp = LocalMLEnvironment.getInstance().getSqlExecutor().fullOuterJoin(inputs[0], inputs[1],
					joidPredicate,
					selectClause);
				break;
			default:
				throw new AkUnsupportedOperationException("Not supported binary op: " + getType());
		}
		this.setOutputTable(outputOp.getOutputTable());
	}
}
