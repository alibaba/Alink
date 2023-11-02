package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.Format;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.HasTimeIntervalDefaultAs3;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public interface WebTrafficIndexParams<T> extends
	HasSelectedCol <T>,
	HasTimeIntervalDefaultAs3 <T>,
	HasKeyCol <T> {

	@NameCn("指标")
	@DescCn("指标")
	ParamInfo <Index> INDEX = ParamInfoFactory
		.createParamInfo("index", Index.class)
		.setDescription("index")
		.setHasDefaultValue(Index.PV)
		.build();

	@NameCn("比特数")
	@DescCn("比特数")
	ParamInfo <Integer> BIT = ParamInfoFactory
		.createParamInfo("bit", Integer.class)
		.setDescription("bit number")
		.setHasDefaultValue(10)
		.build();

	@NameCn("计数方式")
	@DescCn("计数方式")
	ParamInfo <Format> FORMAT = ParamInfoFactory
		.createParamInfo("format", Format.class)
		.setDescription("format")
		.setHasDefaultValue(Format.NORMAL)
		.build();

	default Index getIndex() {
		return get(INDEX);
	}

	default T setIndex(String value) {
		return set(INDEX, ParamUtil.searchEnum(INDEX, value));
	}

	default T setIndex(Index value) {
		return set(INDEX, value);
	}

	default Integer getBit() {
		return get(BIT);
	}

	default T setBit(Integer value) {
		return set(BIT, value);
	}

	default Format getFormat() {
		return get(FORMAT);
	}

	default T setFormat(Format value) {
		return set(FORMAT, value);
	}

	default T setFormat(String value) {
		return set(FORMAT, ParamUtil.searchEnum(FORMAT, value));
	}

	/**
	 * IndexStr for Counter.
	 */
	enum Index {
		/**
		 * <code>PV</code>
		 */
		PV,
		/**
		 * <code>UV</code>;</code>
		 */
		UV,
		/**
		 * <code>UIP</code>
		 */
		UIP,
		/**
		 * <code>CARDINALITY_ESTIMATE_STOCHASTIC</code>
		 */
		CARDINALITY_ESTIMATE_STOCHASTIC,
		/**
		 * <code>CARDINALITY_ESTIMATE_LINEAR</code>
		 */
		CARDINALITY_ESTIMATE_LINEAR,
		/**
		 * <code>CARDINALITY_ESTIMATE_LOGLOG</code>
		 */
		CARDINALITY_ESTIMATE_LOGLOG,
		/**
		 * <code>CARDINALITY_ESTIMATE_ADAPTIVE</code>
		 */
		CARDINALITY_ESTIMATE_ADAPTIVE,
		/**
		 * <code>CARDINALITY_ESTIMATE_HYPERLOGLOG</code>
		 */
		CARDINALITY_ESTIMATE_HYPERLOGLOG,
		/**
		 * <code>CARDINALITY_ESTIMATE_HYPERLOGLOGPLUS</code>
		 */
		CARDINALITY_ESTIMATE_HYPERLOGLOGPLUS,
	}
}
