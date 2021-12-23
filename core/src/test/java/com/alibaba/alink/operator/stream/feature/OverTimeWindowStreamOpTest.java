package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.sql.Timestamp;

public class OverTimeWindowStreamOpTest {

	@Test
	public void test() throws Exception {

		MemSourceStreamOp source = new MemSourceStreamOp(
			new Row[] {
				Row.of(1, "user2", Timestamp.valueOf("2021-01-01 00:01:01"), 20),
				Row.of(2, "user1", Timestamp.valueOf("2021-01-01 00:01:02"), 50),
				Row.of(3, "user2", Timestamp.valueOf("2021-01-01 00:03:01"), 30),
				Row.of(4, "user1", Timestamp.valueOf("2021-01-01 00:06:03"), 60),
				Row.of(5, "user2", Timestamp.valueOf("2021-01-01 00:06:00"), 40),
				Row.of(6, "user2", Timestamp.valueOf("2021-01-01 00:06:00"), 20),
				Row.of(7, "user2", Timestamp.valueOf("2021-01-01 00:07:00"), 70),
				Row.of(8, "user1", Timestamp.valueOf("2021-01-01 00:08:00"), 80),
				Row.of(9, "user1", Timestamp.valueOf("2021-01-01 00:09:00"), 40),
				Row.of(10, "user1", Timestamp.valueOf("2021-01-01 00:10:00"), 20),
				Row.of(11, "user1", Timestamp.valueOf("2021-01-01 00:11:00"), 30),
				Row.of(12, "user1", Timestamp.valueOf("2021-01-01 00:11:00"), 50)
			},
			new String[] {"id", "user", "sell_time", "price"}
		);

		source
			.select("*, CEIL(sell_time TO MINUTE) AS " + ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_NAME)
			.link(
				new OverTimeWindowStreamOp()
					.setTimeCol(ModelStreamUtils.MODEL_STREAM_TIMESTAMP_COLUMN_NAME)
					.setPrecedingTime(1)
					.setClause("COUNT(*) AS " + ModelStreamUtils.MODEL_STREAM_COUNT_COLUMN_NAME)
			)
			.print();
		StreamOperator.execute();

	}
}