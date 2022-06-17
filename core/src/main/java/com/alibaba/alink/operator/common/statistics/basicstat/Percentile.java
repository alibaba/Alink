package com.alibaba.alink.operator.common.statistics.basicstat;

import com.alibaba.alink.common.utils.AlinkSerializable;

public class Percentile implements AlinkSerializable {
	public String colName;
	public String colType;
	public Object[] items = new Object[101];
	public Object median;
	public Object Q1;
	public Object Q3;
	public Object min;
	public Object max;
}
