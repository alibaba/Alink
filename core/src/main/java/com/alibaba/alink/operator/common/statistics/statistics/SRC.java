/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.utils.AlinkSerializable;

/**
 * @author yangxu
 */
public class SRC implements AlinkSerializable {
	public long countTotal;
	public long count;
	public long countMissValue;
	public long countNanValue;
	public long countPositiveInfinity;
	public long countNegativInfinity;
	public double sum;
	public double sum2;
	public double sum3;
	public double sum4;
	public Object min;
	public Object max;
	public double mean;
	public double variance;
	public double standardError;
	public double standardVariance;
	public double skewness;
	public double normL1;
	public double normL2;
	public String dataType;

}
