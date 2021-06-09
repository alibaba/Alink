package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.nlp.HasBatchSize;
import com.alibaba.alink.params.nlp.HasNegative;
import com.alibaba.alink.params.nlp.walk.HasIsToUndigraph;
import com.alibaba.alink.params.nlp.walk.HasSourceCol;
import com.alibaba.alink.params.nlp.walk.HasTargetCol;
import com.alibaba.alink.params.nlp.walk.HasWeightCol;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.HasVectorSizeDv100;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.validators.MinValidator;
import com.alibaba.alink.params.validators.RangeValidator;

public interface LineParams<T> extends
	HasSourceCol <T>,
	HasTargetCol <T>,
	HasIsToUndigraph <T>,
	HasVectorSizeDv100 <T>,
	HasWeightCol <T>,
	HasMaxIterDefaultAs100 <T>,
	HasNegative <T>,
	HasNumThreads <T>,
	HasBatchSize <T> {

	ParamInfo <Order> ORDER = ParamInfoFactory
		.createParamInfo("order", Order.class)
		.setDescription("the order, choose from 1 or 2")
		.setHasDefaultValue(Order.FirstOrder)
		.build();

	default Order getOrder() {return get(ORDER);}

	default T setOrder(Order value) {return set(ORDER, value);}

	default T setOrder(String value) {
		return set(ORDER, ParamUtil.searchEnum(ORDER, value));
	}

	enum Order {
		FirstOrder(1),
		SecondOrder(2);
		private int value;

		Order(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}

	ParamInfo <Double> RHO = ParamInfoFactory
		.createParamInfo("rho", Double.class)
		.setDescription("the learning rate")
		.setHasDefaultValue(0.025)
		.setValidator(new MinValidator <>(0.0))
		.build();

	default Double getRho() {return get(RHO);}

	default T setRho(Double value) {return set(RHO, value);}

	ParamInfo <Double> SAMPLE_RATIO_PER_PARTITION = ParamInfoFactory
		.createParamInfo("sampleRatioPerPartition", Double.class)
		.setDescription("sampleRatioPerPartition")
		.setValidator(new MinValidator <>(0.0))
		.setHasDefaultValue(1.)
		.build();

	default Double getSampleRatioPerPartition() {return get(SAMPLE_RATIO_PER_PARTITION);}

	default T setSampleRatioPerPartition(Double value) {return set(SAMPLE_RATIO_PER_PARTITION, value);}

	ParamInfo <Double> MIN_RHO_RATE = ParamInfoFactory
		.createParamInfo("minRhoRate", Double.class)
		.setDescription("min rho rate")
		.setHasDefaultValue(0.001)
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getMinRhoRate() {
		return get(MIN_RHO_RATE);
	}

	default T setMinRhoRate(Double value) {
		return set(MIN_RHO_RATE, value);
	}
}
