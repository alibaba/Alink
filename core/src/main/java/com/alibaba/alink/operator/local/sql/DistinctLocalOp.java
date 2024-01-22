package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsNull;

import java.sql.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Remove duplicated records.
 */
@NameCn("SQL操作：Distinct")
public final class DistinctLocalOp extends BaseSqlApiLocalOp <DistinctLocalOp> {

	private static final long serialVersionUID = 2774293287356122519L;

	public DistinctLocalOp() {
		this(new Params());
	}

	public DistinctLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor().distinct(inputs[0]).getOutputTable());
	}

}
