package com.alibaba.alink.operator.common.io.types;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;

public class JdbcTypeConverterTest extends AlinkTestBase {
	@Test
	public void testMutualConversion() {
		int[] types = new int[] {Types.VARCHAR, Types.BOOLEAN,
			Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.FLOAT,
			Types.DOUBLE, Types.DATE, Types.TIME, Types.TIMESTAMP, Types.DECIMAL, Types.VARBINARY};

		for (int type : types) {
			TypeInformation <?> flinkType = JdbcTypeConverter.getFlinkType(type);
			int sqlType = JdbcTypeConverter.getIntegerSqlType(flinkType);
			Assert.assertEquals(type, sqlType);
		}
	}

	@Test
	public void testSpecialStringType() {
		int[] types = new int[] {Types.LONGVARCHAR, Types.NULL};

		for (int type : types) {
			TypeInformation <?> flinkType = JdbcTypeConverter.getFlinkType(type);
			Assert.assertEquals(flinkType, BasicTypeInfo.STRING_TYPE_INFO);
		}
	}

	@Test(expected = AkIllegalArgumentException.class)
	public void testUnsupportedSqlType() {
		JdbcTypeConverter.getFlinkType(Types.TIME_WITH_TIMEZONE);
	}

	@Test(expected = AkIllegalOperatorParameterException.class)
	public void testUnsupportedFlinkType() {
		JdbcTypeConverter.getIntegerSqlType(TypeInformation.of(JdbcTypeConverter.class));
	}
}