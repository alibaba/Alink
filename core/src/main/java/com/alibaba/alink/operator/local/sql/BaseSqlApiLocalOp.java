package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;

/**
 * The base class of all local sql operators.
 * <p>
 * The local sql operators apply the sql operation (select, where, group by, join, etc.) on their input local
 * operators.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
public abstract class BaseSqlApiLocalOp<T extends BaseSqlApiLocalOp <T>> extends LocalOperator <T> {

	private static final long serialVersionUID = -6867025133749819154L;

	public BaseSqlApiLocalOp(Params params) {
		super(params);
	}
}
