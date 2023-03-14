package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.LofDetectorParams;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Random;

public class LofDetectorTest extends AlinkTestBase {

	private static final double EPS = 1e-12;

	@Test
	public void testCalcPercentile() {
		Assert.assertEquals(2.5, LofDetector.calcPercentile(50., new double[] {3, 4, 1, 2}), EPS);
		Assert.assertEquals(1.75, LofDetector.calcPercentile(25., new double[] {3, 4, 1, 2}), EPS);
		Assert.assertEquals(3.25, LofDetector.calcPercentile(75., new double[] {3, 4, 1, 2}), EPS);
		Assert.assertEquals(3., LofDetector.calcPercentile(50., new double[] {3, 4, 1, 2, 5}), EPS);
		Assert.assertEquals(-29.97008,
			LofDetector.calcPercentile(20, new double[] {-0.9821, -1.0370, -73.3697, -0.9821}), EPS);
	}

	@Test
	public void testDetectDefaultThreshold() throws Exception {
		Row[] data = new Row[] {
			Row.of("-1.1"),
			Row.of("0.2"),
			Row.of("101.1"),
			Row.of("0.3"),
		};
		MTable mtable = new MTable(data, "vec vector");
		TableSchema schema = TableUtil.schemaStr2Schema("mtable string");

		Params params = new Params()
			.set(HasInputMTableCol.INPUT_MTABLE_COL, "mtable")
			.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, "dummy")
			.set(HasVectorCol.VECTOR_COL, "vec")
			.set(LofDetectorParams.NUM_NEIGHBORS, 2);
		LofDetector detector = new LofDetector(schema, params);
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detector.detect(mtable, false);

		//noinspection unchecked
		Tuple3 <Boolean, Double, Map <String, String>>[] expected = new Tuple3[] {
			Tuple3.of(false, 0.9821428571428571, Collections.singletonMap("lof", "0.9821428571428571")),
			Tuple3.of(false, 1.0370370370370372, Collections.singletonMap("lof", "1.0370370370370372")),
			Tuple3.of(true, 73.36970899470899, Collections.singletonMap("lof", "73.36970899470899")),
			Tuple3.of(false, 0.9821428571428571, Collections.singletonMap("lof", "0.9821428571428571"))
		};

		Assert.assertEquals(expected.length, results.length);
		for (int i = 0; i < results.length; i += 1) {
			Assert.assertEquals(expected[i].f0, results[i].f0);
			Assert.assertEquals(expected[i].f1, results[i].f1, EPS);
			Assert.assertEquals(expected[i].f2, results[i].f2);
		}
	}

	@Test
	public void testDetectWithSameSamples() throws Exception {
		Row[] data = new Row[] {
			Row.of("-1.1"),
			Row.of("-1.1"),
			Row.of("-1.1"),
			Row.of("-1.1"),
			Row.of("0.2"),
			Row.of("101.1"),
			Row.of("0.3"),
		};
		MTable mtable = new MTable(data, "vec vector");
		TableSchema schema = TableUtil.schemaStr2Schema("mtable string");

		Params params = new Params()
			.set(HasInputMTableCol.INPUT_MTABLE_COL, "mtable")
			.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, "dummy")
			.set(HasVectorCol.VECTOR_COL, "vec")
			.set(LofDetectorParams.NUM_NEIGHBORS, 2);
		LofDetector detector = new LofDetector(schema, params);
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detector.detect(mtable, false);

		//noinspection unchecked
		Tuple3 <Boolean, Double, Map <String, String>>[] expected = new Tuple3[] {
			Tuple3.of(false, 1., Collections.singletonMap("lof", "1.0")),
			Tuple3.of(false, 1., Collections.singletonMap("lof", "1.0")),
			Tuple3.of(false, 1., Collections.singletonMap("lof", "1.0")),
			Tuple3.of(false, 1., Collections.singletonMap("lof", "1.0")),
			Tuple3.of(true, 6.75E17, Collections.singletonMap("lof", "6.75E17")),
			Tuple3.of(true, 74.7037037037037, Collections.singletonMap("lof", "74.7037037037037")),
			Tuple3.of(true, 6.75E17, Collections.singletonMap("lof", "6.75E17"))
		};

		Assert.assertEquals(expected.length, results.length);
		for (int i = 0; i < results.length; i += 1) {
			Assert.assertEquals(expected[i].f0, results[i].f0);
			Assert.assertEquals(expected[i].f1, results[i].f1, EPS);
			Assert.assertEquals(expected[i].f2, results[i].f2);
		}
	}

	@Test
	public void testDetectSparseVector() throws Exception {
		int n = 30;
		int len = 20;
		double sparsity = 0.1;
		Random random = new Random(0);
		Row[] data = new Row[n];
		for (int k = 0; k < n; k += 1) {
			SparseVector vector = new SparseVector(len);
			int lastIdx = 0;
			for (int i = 0; i < len * sparsity; i += 1) {
				int idx = lastIdx + random.nextInt(len - lastIdx);
				vector.set(idx, random.nextDouble());
				lastIdx = idx;
				if (lastIdx == len - 1) {
					break;
				}
			}
			data[k] = Row.of(vector);
		}

		MTable mtable = new MTable(data, "vec vector");
		TableSchema schema = TableUtil.schemaStr2Schema("mtable string");

		Params params = new Params()
			.set(HasInputMTableCol.INPUT_MTABLE_COL, "mtable")
			.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, "dummy")
			.set(HasVectorCol.VECTOR_COL, "vec")
			.set(LofDetectorParams.NUM_NEIGHBORS, 2);
		LofDetector detector = new LofDetector(schema, params);
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detector.detect(mtable, false);

		for (Tuple3 <Boolean, Double, Map <String, String>> result : results) {
			System.out.println(result);
		}
		Assert.assertEquals(data.length, results.length);
	}
}
