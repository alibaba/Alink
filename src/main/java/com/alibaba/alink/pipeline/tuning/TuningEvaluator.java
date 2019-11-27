package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.evaluation.HasMetricName;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TuningEvaluator<T extends TuningEvaluator <T>> implements HasMetricName <T>, WithParams<T> {
	protected Params params;

	public TuningEvaluator(Params params) {
		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}
	}

	static ParamInfo findParamInfo(Class <? extends WithParams> param, String metricName) {
		return Arrays
			.stream(param.getFields())
			.filter(x -> x.getType().equals(ParamInfo.class))
			.map(x -> {
				try {
					return (ParamInfo) x.get(null);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
					//pass
				}

				return null;
			})
			.filter(x -> x != null && x.getValueClass().equals(Double.class))
			.flatMap(x -> {
				Stream <Tuple2 <String, ParamInfo>> ret = Stream.of(Tuple2.of(x.getName().trim().toLowerCase(), x));
				if (x.getAlias() == null) {
					return ret;
				} else {
					return Stream.concat(Arrays.stream(x.getAlias())
							.map(y -> Tuple2.of(y.trim().toLowerCase(), x)),
						ret);
				}
			})
			.collect(Collectors.groupingBy(x -> x.f0,
				Collectors.mapping(x -> x.f1, Collectors.reducing((y, z) -> y))
				)
			)
			.get(metricName.trim().toLowerCase())
			.orElseThrow(() -> new RuntimeException("Can not find " + metricName));
	}

	@Override
	public Params getParams() {
		if (null == this.params) {
			this.params = new Params();
		}

		return params;
	}

	public abstract double evaluate(BatchOperator in);

	public abstract boolean isLargerBetter();
}
