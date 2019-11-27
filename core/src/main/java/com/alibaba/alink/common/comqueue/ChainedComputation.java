package com.alibaba.alink.common.comqueue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Multiple computeFunction chained into one.
 */
class ChainedComputation extends ComputeFunction {
	List <ComputeFunction> computations = new ArrayList <>();

	public ChainedComputation add(ComputeFunction computation) {
		computations.add(computation);

		return this;
	}

	@Override
	public void calc(ComContext context) {
		for (ComputeFunction computation : computations) {
			computation.calc(context);
		}
	}

	public String name() {
		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append("chained computation@");

		stringBuilder.append(computations
			.stream()
			.map(x -> x.getClass().getSimpleName())
			.collect(Collectors.joining("->"))
		);

		return stringBuilder.toString();
	}
}
