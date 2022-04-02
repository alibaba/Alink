package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * The base class of all batch sql operators.
 * <p>
 * The batch sql operators apply the sql operation (select, where, group by, join, etc.) on their
 * input batch operators.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
public abstract class BaseSqlApiBatchOp<T extends BaseSqlApiBatchOp <T>> extends BatchOperator <T> {

	private static final long serialVersionUID = 6196444419989031059L;

	public BaseSqlApiBatchOp(Params params) {
		super(params);
	}
}
