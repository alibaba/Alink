package com.alibaba.alink.operator.common.statistics.basicstatistic;


import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class SummaryDataConverterTest extends AlinkTestBase {

	@Test
	public void testObjectVectorToString() {
		Object[] values = new Object[]{
			1, 2.0, 3L, true, new Timestamp(5), null, new BigDecimal("10.0")
		};
		String jsonStr = SummaryDataConverter.objectVectorToString(values);
		System.out.println(jsonStr);
		Object[] ojs = SummaryDataConverter.stringToObjectVector(jsonStr);
		for(int i=0; i<ojs.length; i++) {
			if(ojs[i] != null) {
				System.out.println("i: " + i + " " + ojs[i] + " " + ojs[i].getClass());
			} else {
				System.out.println("i: " + i + " null");
			}
		}
	}

}