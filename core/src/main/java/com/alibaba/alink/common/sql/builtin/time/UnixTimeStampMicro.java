package com.alibaba.alink.common.sql.builtin.time;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class UnixTimeStampMicro extends ScalarFunction {

	@Override
	public boolean isDeterministic() {
		return false;
	}

	public Long eval(Timestamp in) {
		if (in == null) {
			return null;
		}

		// when select * from ( select * from xxx), zone change from 8 to UTC. so need set to UTC
		LocalDateTime ts = in.toLocalDateTime();
		return ts.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
	}
}
