package com.alibaba.alink.operator.common.statistics.newstat;

import com.alibaba.alink.operator.common.statistics.statistics.DateMeasureIterator;
import com.alibaba.alink.operator.common.statistics.statistics.NumberMeasureIterator;
import com.alibaba.alink.operator.common.statistics.statistics.TopKIterator;
import junit.framework.TestCase;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;

public class NumberIteratorTest extends TestCase {

	@Test
	public void test() {
		NumberMeasureIterator <Double> md = new NumberMeasureIterator <>();
		for (Double d = 1.0; d < 10000001.0; d++) {
			md.visit(d);
		}
		System.out.println(md);

		NumberMeasureIterator <Integer> mi = new NumberMeasureIterator <>();
		for (Integer d = 1; d < 10000001; d++) {
			mi.visit(d);
		}
		System.out.println(mi);
	}

	@Test
	public void testDate() {
		DateMeasureIterator <Date> di = new DateMeasureIterator <>();
		for (long d = 0; d <= 100000 * 1000; d += 1000) {
			di.visit(new Date(d));
		}
		System.out.println(di);
		DateMeasureIterator <Timestamp> ti = new DateMeasureIterator <>();
		for (long d = 0; d <= 100000 * 1000; d += 1000) {
			ti.visit(new Timestamp(d));
		}
		System.out.println(ti);
	}

	@Test
	public void testTopK() {
		TopKIterator <Double> md = new TopKIterator <>(10, 10);
		for (Double d = 1.0; d < 10000001.0; d++) {
			md.visit(d);
		}
		System.out.println(md);

		TopKIterator <Integer> mi = new TopKIterator <>(10, 10);
		for (Integer d = 1; d < 10000001; d++) {
			mi.visit(d);
		}
		System.out.println(mi);
	}

}