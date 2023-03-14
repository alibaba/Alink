package com.alibaba.alink.operator.common.timeseries;

import com.alibaba.alink.operator.common.timeseries.TimeSeriesMapper;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;

public class TimeSeriesMapperTest extends AlinkTestBase {

	@Test
	public void test() {
		Timestamp[] histoTimes = new Timestamp[] {
			new Timestamp(117, 1, 1, 2, 0, 0, 0),
			new Timestamp(117, 2, 1, 2, 0, 0, 0),
			new Timestamp(117, 2, 29, 2, 0, 0, 0)
		};
		for (Timestamp t : histoTimes) {
			System.out.println(t);
		}
		Timestamp[] predictTimes = TimeSeriesMapper.getPredictTimes(histoTimes, 4);
		for (Timestamp t : predictTimes) {
			System.out.println(t);
		}
	}

	@Test
	public void test2() {
		Timestamp[] histoTimes = new Timestamp[] {
			new Timestamp(117, 1, 1, 1, 0, 0, 0),
			new Timestamp(117, 2, 1, 2, 0, 0, 0),
			new Timestamp(117, 3, 1, 2, 0, 0, 0)
		};

		Timestamp[] predictTimes = TimeSeriesMapper.getPredictTimes(histoTimes, 4);
		for (Timestamp t : predictTimes) {
			System.out.println(t);
		}
	}

}