package com.alibaba.alink.operator.common.statistics.basicstat;

import java.io.Serializable;

public class Quantile implements Serializable {
	private static final long serialVersionUID = 5690612202964994601L;
	public String colName;
	public String colType;
	public int q;
	public Object[] items = null;
}
