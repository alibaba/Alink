package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.annotation.PublicEvolving;

@FunctionalInterface
@PublicEvolving
public interface FourFunction<A, B, C, D, R> {
	R apply(A var1, B var2, C var3, D var4);
}

