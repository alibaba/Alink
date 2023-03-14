package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple5;

import com.alibaba.alink.common.exceptions.AkUnimplementedOperationException;

public class ValueDistUtils {
	public static Tuple5 <Double, Double,Double, Double, Integer> getValueDistSummary(ValueDist valueDist) {
		if (valueDist instanceof ValueDistFunc) {
			ValueDistFunc valueDistFunc = (ValueDistFunc) valueDist;
			double mean = valueDistFunc.getMean();
			double sd = valueDistFunc.getSd();
			return Tuple5.of(mean, sd, null,null,null);
		} else if (valueDist instanceof ValueDistInteger) {
			ValueDistInteger valueDistFunc = (ValueDistInteger) valueDist;
			double lower = ((Number) valueDistFunc.get(0)).doubleValue();
			double upper = ((Number) valueDistFunc.get(1)).doubleValue();
			double mean = (lower + upper) / 2;
			double sd = (upper - lower);
			return Tuple5.of(mean, sd, lower, upper,1);
		} else if (valueDist instanceof ValueDistLong) {
			ValueDistLong valueDistFunc = (ValueDistLong) valueDist;
			double lower = ((Number) valueDistFunc.get(0)).doubleValue();
			double upper = ((Number) valueDistFunc.get(1)).doubleValue();
			double mean = (lower + upper) / 2;
			double sd = (upper - lower);
			return Tuple5.of(mean, sd, lower, upper,1);
		} else {
			throw new AkUnimplementedOperationException(
				"Cannot get mean and standard of ValueArray type hyper parameters. Will add this calculation later");
		}
	}
}
