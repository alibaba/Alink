package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class OverCountWindowStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		List <Row> sourceFrame = Arrays.asList(
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:00"), 10.0, 10, new BigDecimal("11.0")),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:00:00"), 20.0, 20, new BigDecimal("12.0")),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:11"), 30.0, 30, new BigDecimal("13.0")),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:00:15"), 40.0, 40, new BigDecimal("14.0")),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:01:00"), 50.0, 50, new BigDecimal("15.0")),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 12:01:00"), 60.0, 60, new BigDecimal("16.0")),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 12:01:23"), 70.0, 70, new BigDecimal("17.0")),
			Row.of("Mary", Timestamp.valueOf("2021-11-11 12:02:00"), 80.0, 80, new BigDecimal("18.0")),
			Row.of("Liz", Timestamp.valueOf("2021-11-11 12:02:10"), 90.0, 90, new BigDecimal("19.0")),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:02:20"), 100.0, 100, new BigDecimal("20.0")),
			Row.of("Bob", Timestamp.valueOf("2021-11-11 12:02:25"), 110.0, 110, new BigDecimal("21.0"))
		);

		StreamOperator <?> streamSource = new MemSourceStreamOp(
			sourceFrame,
			new String[] {"user", "time1", "money", "money_int", "money_decimal"});

		String clause =
			"avg_preceding(money) as avg_money, "
				+ "CONCAT_AGG_PRECEDING(money) as t1, "
				+ "LAST_DISTINCT(money) as t2, "
				+ "LAST_DISTINCT_including_null(money) as t2_i, "
				+ "LAST_DISTINCT(time1, money) as t3,"
				+ "LAST_DISTINCT_including_null(time1, money) as t3_i,"
				+ "LAST_VALUE(money) as t4,"
				+ "LAST_VALUE_including_null(money) as t4_i,"
				+ "LAST_TIME(money) as t5,"
				+ "STDDEV_SAMP(money) as stdsam, "
				+ "STDDEV_POP(money) as stdpop, "
				+ "VAR_SAMP(money) as varsamp, "
				+ "VAR_POP(money) as varpop, "
				+ "count(money) as countip, "
				+ "avg(money) as avgip, "
				+ "min(money) as minip, "
				+ "max(money) as maxip, "
				+ "lag(money) as lagip, "
				+ "sum_last(money) as sum_lastip, "
				+ "listagg(money) as listaggip, "
				+ "mode(money) as modeip, "
				+ "square_sum(money) as square_sumip, "
				+ "median(money) as medianip,"
				+ "freq(money) as freqip, "
				+ "is_exist(money) as is_existip,"
				+ "STDDEV_SAMP_PRECEDING(money) as stdsam_pre, "
				+ "STDDEV_POP_PRECEDING(money) as stdpop_pre, "
				+ "VAR_SAMP_PRECEDING(money) as varsamp_pre, "
				+ "VAR_POP_PRECEDING(money) as varpop_pre, "
				+ "count_PRECEDING(money) as countip_pre, "
				+ "avg_PRECEDING(money) as avgip_pre, "
				+ "min_PRECEDING(money) as minip_pre, "
				+ "max_PRECEDING(money) as maxip_pre, "
				+ "lag_including_null(money) as lagip_pre, "
				+ "listagg_PRECEDING(money) as listaggip_pre, "
				+ "mode_PRECEDING(money) as modeip_pre, "
				+ "square_sum_PRECEDING(money) as square_sumip_pre, "
				+ "median_PRECEDING(money) as medianip_pre,"
				+ "freq_PRECEDING(money) as freqip_pre,"
				+ "lag(money, 2) as lagip_2, "
				+ "lag(money, 2, 3.5) as lagip_3,"
				+ "lag(money_decimal, 2, 3.5) as lag_md_3,"
				+ "money";

		System.out.println(clause);

		StreamOperator <?> op = new OverCountWindowStreamOp()
			.setGroupCols("user")
			.setTimeCol("time1")
			.setPrecedingRows(3)
			.setClause(clause)
			.setReservedCols("user", "time1", "money");

		streamSource.link(op).print();

		StreamOperator.execute();
	}

}