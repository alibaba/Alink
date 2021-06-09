package com.alibaba.alink.operator.common.utils;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.display;
import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.displayList;

public class PrettyDisplayUtilsTest extends AlinkTestBase {

	public static double[] generateRandomDoubleArray(int n) {
		double[] arr = new double[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = RandomUtils.nextDouble(0, 1e10);
		}
		return arr;
	}

	@Test
	public void testPrimitives() {
		System.out.println(display(123));
		System.out.println(display(12314123123L));
		System.out.println(display(1431.423));
		System.out.println(display(1234.4123456789));
		System.out.println(display("12314"));
	}

	@Test
	public void testList() {
		System.out.println(display(Arrays.asList("123", "456", "789", "1011", "abc", "adf", "efa")));
		System.out.println(displayList(Arrays.asList("123", "456", "789", "1011", "abc", "adf", "efa"), true));
		System.out.println(display(Arrays.asList(ArrayUtils.toObject(generateRandomDoubleArray(10)))));
	}

	@Test
	public void test2DList() {
		List <String> l = Arrays.asList("123", "456", "789", "1011", "abc", "adf", "efa");
		List <List <String>> ll = new ArrayList <>();
		for (int i = 0; i < 10; i += 1) {
			ll.add(l);
		}
		System.out.println(displayList(ll, true));
	}

	@Test
	public void testDenseVector() {
		System.out.println(PrettyDisplayUtils.displayDenseVector(new DenseVector(generateRandomDoubleArray(10))));

		List <DenseVector> denseVectors = new ArrayList <>();
		for (int i = 0; i < 10; i += 1) {
			denseVectors.add(new DenseVector(generateRandomDoubleArray(10)));
		}
		System.out.println(PrettyDisplayUtils.displayList(denseVectors, true));
	}

	@Test
	public void prependStringWithIndent() {
		List <DenseVector> denseVectors = new ArrayList <>();
		for (int i = 0; i < 10; i += 1) {
			denseVectors.add(new DenseVector(generateRandomDoubleArray(10)));
		}
		String content = displayList(denseVectors, true);
		content = PrettyDisplayUtils.prependStringWithIndent(content, "centers: ");
		System.out.println(content);
	}

	@Test
	public void testMap() {
		Map <String, Object> m = new LinkedHashMap <>();
		for (int i = 0; i < 10; i += 1) {
			m.put(String.valueOf(i), new DenseVector(generateRandomDoubleArray(10)));
		}
		m.put("4", "Hello World");
		System.out.println(PrettyDisplayUtils.displayMap(m, 3, true));
		System.out.println(PrettyDisplayUtils.displayMap(m, 3, false));
		System.out.println(PrettyDisplayUtils.display(m));
	}

	@Test
	public void testTable() {
		int nRows = 20;
		int nCols = 15;
		Double[][] table = new Double[nRows][nCols];
		for (int i = 0; i < nRows; i += 1) {
			for (int j = 0; j < nCols; j += 1) {
				table[i][j] = RandomUtils.nextDouble(0.0, 9999999999999999.0);
			}
		}

		String[] rowNames = new String[nRows];
		for (int i = 0; i < nRows; i += 1) {
			rowNames[i] = "row" + i;
		}

		String[] colNames = new String[nCols];
		for (int i = 0; i < nCols; i += 1) {
			colNames[i] = "col" + i;
		}

		String ret = PrettyDisplayUtils.displayTable(table, nRows, nCols, rowNames, colNames, null, 3, 3);
		System.out.println(ret);

		System.out.println(PrettyDisplayUtils.displayTable(table, nRows, nCols, rowNames, colNames, "features", 2, 1));

		System.out.println(PrettyDisplayUtils.displayTable(table, nRows, nCols, null, colNames, "features", 2, 1));

		System.out.println(PrettyDisplayUtils.displayTable(table, nRows, nCols, rowNames, null, "features", 2, 1));

		System.out.println(PrettyDisplayUtils.displayTable(table, nRows, nCols, null, null, "features", 2, 1));
	}

	@Test
	public void testHeadline() {
		System.out.println(PrettyDisplayUtils.displayHeadline("IMPORTANCE", '-'));
		System.out.println(PrettyDisplayUtils.displayHeadline("convergence info", '='));
	}
}
