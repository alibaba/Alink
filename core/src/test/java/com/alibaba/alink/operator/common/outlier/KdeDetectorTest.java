package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasKDEKernelType.KernelType;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.KdeDetectorParams;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class KdeDetectorTest {

	private static final double EPS = 1e-4;

	@Test
	public void testDetectLinearKernel() throws Exception {
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
			.set(KdeDetectorParams.BANDWIDTH, 4.)
			.set(KdeDetectorParams.OUTLIER_THRESHOLD, 10.0)
			.set(KdeDetectorParams.KDE_KERNEL_TYPE, KernelType.LINEAR);

		KdeDetector detector = new KdeDetector(schema, params);
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detector.detect(mtable, false);

		//noinspection unchecked
		Tuple2 <Boolean, Double>[] expected = new Tuple2[] {
			Tuple2.of(false, 6.88172),
			Tuple2.of(false, 6.03773),
			Tuple2.of(true, 16.0),
			Tuple2.of(false, 6.09523)
		};

		Assert.assertEquals(expected.length, results.length);
		for (int i = 0; i < results.length; i += 1) {
			Assert.assertEquals(expected[i].f0, results[i].f0);
			Assert.assertEquals(expected[i].f1, results[i].f1, EPS);
		}
	}

	@Test
	public void testDetectGaussianKernel() throws Exception {
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
			.set(KdeDetectorParams.BANDWIDTH, 4.)
			.set(KdeDetectorParams.OUTLIER_THRESHOLD, 15.0)
			.set(KdeDetectorParams.KDE_KERNEL_TYPE, KernelType.GAUSSIAN);

		KdeDetector detector = new KdeDetector(schema, params);
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detector.detect(mtable, false);

		//noinspection unchecked
		Tuple2 <Boolean, Double>[] expected = new Tuple2[] {
			Tuple2.of(false, 13.88162),
			Tuple2.of(false, 13.60336),
			Tuple2.of(true, 40.10605),
			Tuple2.of(false, 13.64023)
		};

		Assert.assertEquals(expected.length, results.length);
		for (int i = 0; i < results.length; i += 1) {
			Assert.assertEquals(expected[i].f0, results[i].f0);
			Assert.assertEquals(expected[i].f1, results[i].f1, EPS);
		}
	}

}
