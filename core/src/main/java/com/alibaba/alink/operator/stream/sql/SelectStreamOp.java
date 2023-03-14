package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.sql.SelectUtils;
import com.alibaba.alink.operator.common.sql.SimpleSelectMapper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.sql.SelectParams;

/**
 * Select the fields of a stream operator.
 */
@NameCn("SQL操作：Select")
@NameEn("SQL：Select")
public final class SelectStreamOp extends BaseSqlApiStreamOp <SelectStreamOp>
	implements SelectParams <SelectStreamOp> {

	private static final long serialVersionUID = 7401063240614374140L;

	public SelectStreamOp() {
		this(new Params());
	}

	public SelectStreamOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public SelectStreamOp(Params params) {
		super(params);
	}

	@Override
	public SelectStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		String[] colNames = in.getColNames();

		String clause = getClause();
		String newClause = SelectUtils.convertRegexClause2ColNames(colNames, clause);

		if (SelectUtils.isSimpleSelect(newClause, colNames)) {
			this.setOutputTable(
				in.link(new SimpleSelectStreamOp()
					.setClause(newClause)
					.setMLEnvironmentId(in.getMLEnvironmentId())
				).getOutputTable());
		} else {
			this.setOutputTable(StreamSqlOperators.select(in, newClause).getOutputTable());
		}

		return this;
	}

	@Internal
	private class SimpleSelectStreamOp extends MapStreamOp <SimpleSelectStreamOp>
		implements SelectParams <SimpleSelectStreamOp> {

		public SimpleSelectStreamOp() {
			this(null);
		}

		public SimpleSelectStreamOp(Params param) {
			super(SimpleSelectMapper::new, param);
		}
	}
}

