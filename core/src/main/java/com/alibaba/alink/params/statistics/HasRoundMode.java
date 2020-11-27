package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * round mode.
 */
public interface HasRoundMode<T> extends WithParams <T> {
	ParamInfo <RoundMode> ROUND_MODE = ParamInfoFactory
		.createParamInfo("roundMode", RoundMode.class)
		.setDescription("when q is the group size, k is the k-th group, total is the sample size, " +
			"then the index of k-th q-quantile is (1.0 / q) * (total - 1) * k. " +
			"if convert index from double to long, it use round mode." +
			"<ul>" +
			"<li>round: [index]</li>" +
			"<li>ceil: ⌈index⌉</li>" +
			"<li>floor: ⌊index⌋</li>" +
			"</ul>" +
			"<p>")
		.setHasDefaultValue(RoundMode.ROUND)
		.build();

	default RoundMode getRoundMode() {
		return get(ROUND_MODE);
	}

	default T setRoundMode(String value) {
		return set(ROUND_MODE, ParamUtil.searchEnum(ROUND_MODE, value));
	}

	default T setRoundMode(RoundMode value) {
		return set(ROUND_MODE, value);
	}

	enum RoundMode implements RoundType, Serializable {
		/**
		 * ⌈a⌉
		 */
		CEIL(new RoundType() {
			private static final long serialVersionUID = -2261719519392607515L;

			@Override
			public long calc(double a) {
				return (long) Math.ceil(a);
			}
		}),

		/**
		 * ⌊a⌋
		 */
		FLOOR(new RoundType() {
			private static final long serialVersionUID = 3745431503726311565L;

			@Override
			public long calc(double a) {
				return (long) Math.floor(a);
			}
		}),

		/**
		 * [a]
		 */
		ROUND(new RoundType() {
			private static final long serialVersionUID = -5266768529556614677L;

			@Override
			public long calc(double a) {
				return Math.round(a);
			}
		});

		private final RoundType roundType;

		RoundMode(RoundType roundType) {
			this.roundType = roundType;
		}

		@Override
		public String toString() {
			return super.name();
		}

		@Override
		public long calc(double a) {
			/**
			 * 0.1 * (8.0 - 1.0) * 10.0 = 7.000000000000001,
			 * we hold 14 digits after the decimal point to avoid this situation
			 */
			BigDecimal bigDecimal = new BigDecimal(a);
			return roundType.calc(bigDecimal.setScale(14,
				BigDecimal.ROUND_HALF_UP).doubleValue());
		}
	}

	interface RoundType extends Serializable {
		long calc(double a);
	}
}
