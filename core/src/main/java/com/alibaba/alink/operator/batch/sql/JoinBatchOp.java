package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.JoinParams;

/**
 * Join two batch operators.
 */
@InputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：Join")
@NameEn("SQL Join Operation")
public final class JoinBatchOp extends BaseSqlApiBatchOp <JoinBatchOp>
	implements JoinParams <JoinBatchOp> {

	private static final long serialVersionUID = -5284150849586086589L;

	public JoinBatchOp() {
		this(new Params());
	}

	public JoinBatchOp(String joinPredicate) {
		this(joinPredicate, "*");
	}

	public JoinBatchOp(String joinPredicate, String selectClause) {
		this(new Params()
			.set(JOIN_PREDICATE, joinPredicate)
			.set(SELECT_CLAUSE, selectClause));
	}

	public JoinBatchOp(Params params) {
		super(params);
	}

	@Override
	public JoinBatchOp linkFrom(BatchOperator <?>... inputs) {
		String selectClause = "*";
		if (this.getParams().contains(JoinParams.SELECT_CLAUSE)) {
			selectClause = this.getParams().get(JoinParams.SELECT_CLAUSE);
		}
		String joidPredicate = this.getParams().get(JoinParams.JOIN_PREDICATE);

		BatchOperator outputOp = null;
		switch (getType()) {
			case JOIN:
				outputOp = BatchSqlOperators.join(inputs[0], inputs[1], joidPredicate, selectClause);
				break;
			case LEFTOUTERJOIN:
				outputOp = BatchSqlOperators.leftOuterJoin(inputs[0], inputs[1], joidPredicate, selectClause);
				break;
			case RIGHTOUTERJOIN:
				outputOp = BatchSqlOperators.rightOuterJoin(inputs[0], inputs[1], joidPredicate, selectClause);
				break;
			case FULLOUTERJOIN:
				outputOp = BatchSqlOperators.fullOuterJoin(inputs[0], inputs[1], joidPredicate, selectClause);
				break;
			default:
				throw new AkUnsupportedOperationException("Not supported binary op: " + getType());
		}
		this.setOutputTable(outputOp.getOutputTable());
		return this;
	}
}
