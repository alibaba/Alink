package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

import java.io.Serializable;

/**
 * @author lqb
 * @date 2018/09/20
 */
public abstract class AbstractCounter implements Serializable {
	private static final long serialVersionUID = 5596401909982410203L;
	protected WebTrafficIndexParams.Index counter;

	public abstract void visit(Object obj);

	public abstract long count();

	public abstract AbstractCounter merge(AbstractCounter counter);

}
