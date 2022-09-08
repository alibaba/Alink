package com.alibaba.alink.operator.local;

import org.apache.flink.types.Row;

import com.google.common.collect.HashMultiset;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Utils {
	/**
	 * Check equality of two lists of rows.
	 * <p>
	 * Two lists must have the same number of elements to be equal. The equality of {@link Row} depends on {@link
	 * Row#equals}. Note that floating-point numbers cannot be compared with tolerance.
	 *
	 * @param expected Expected list of rows
	 * @param actual   Actual list of rows
	 */
	public static void assertListRowEqualWithoutOrder(String message, List <Row> expected, List <Row> actual) {
		HashMultiset <Row> expectedSet = HashMultiset.create(expected);
		HashMultiset <Row> actualSet = HashMultiset.create(actual);
		Assert.assertEquals(message, expectedSet, actualSet);
	}

	public static void assertListRowEqualWithoutOrder(List <Row> expected, List <Row> actual) {
		assertListRowEqualWithoutOrder(null, expected, actual);
	}

	public static void assertRowArrayWithoutOrder(String message, Row[] expected, Row[] actual) {
		assertListRowEqualWithoutOrder(message, Arrays.asList(expected), Arrays.asList(actual));

	}

	public static void assertRowArrayWithoutOrder(Row[] expected, Row[] actual) {
		assertRowArrayWithoutOrder(null, expected, actual);
	}

	public static void assertListRowEqual(String message,
										  List <Row> expected,
										  List <Row> actual,
										  int[] sortIndices) {
		actual.sort(new Comparator <Row>() {
			@Override
			public int compare(Row row1, Row row2) {
				return Utils.compare(row1, row2, sortIndices);
			}
		});

		Assert.assertEquals("expected size is not equal with actual size.", expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			Row expectedO = expected.get(i);
			Row actualO = actual.get(i);

			Assert.assertEquals(
				String.format("expected size is not equal with actual size. \nExpected :%s \nActual   :%s\n",
					expectedO, actualO),
				expectedO.getArity(), actualO.getArity());

			for (int j = 0; j < expectedO.getArity(); j++) {
				Assert.assertEquals(
					String.format("expected value is not equal with actual value. \nExpected :%s \nActual   :%s\n",
						expectedO, actualO),
					expectedO.getField(j), actualO.getField(j));
			}
		}
		Assert.assertArrayEquals(message, expected.toArray(), actual.toArray());
	}

	public static void assertListRowEqual(List <Row> expected,
										  List <Row> actual,
										  int[] sortIndices) {
		assertListRowEqual(null, expected, actual, sortIndices);
	}

	public static void assertListRowEqual(List <Row> expected,
										  List <Row> actual,
										  int sortIdx) {
		assertListRowEqual(null, expected, actual, new int[] {sortIdx});
	}

	private static int compare(Row row1, Row row2, int[] sortIndices) {
		for (int idx : sortIndices) {
			int compareR = compare(row1, row2, idx);
			if (compareR != 0) {
				return compareR;
			}
		}
		return 0;
	}

	private static int compare(Row row1, Row row2, int sortIdx) {
		Object o1 = row1.getField(sortIdx);
		Object o2 = row2.getField(sortIdx);

		if (o1 == null && o2 == null) {
			return 0;
		} else if (o1 == null) {
			return 1;
		} else if (o2 == null) {
			return -1;
		}

		return ((Comparable) o1).compareTo(o2);
	}

}
