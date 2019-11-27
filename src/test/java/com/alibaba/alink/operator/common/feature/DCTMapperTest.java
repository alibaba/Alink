package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.params.feature.DCTParams;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

public class DCTMapperTest {

	@Test
	public void test() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING()});

		DCTMapper dctMapper = new DCTMapper(schema, new Params().set(DCTParams.SELECTED_COL, "vec"));

		DCTMapper inverseDCTMapper = new DCTMapper(schema,
			new Params().set(DCTParams.SELECTED_COL, "vec").set(DCTParams.INVERSE, true));

		String[] vectors = new String[] {
			"1.0 2.0 3.0 4.0 5.0",
			"1.0 2.0 1.0 2.0",
			"1.0 100000.0 -5000.0 0.1 0.0000005"
		};

		for (String vector : vectors) {
			assertTrue(
				VectorUtil.parseDense((String) inverseDCTMapper.map(dctMapper.map(Row.of(vector))).getField(0))
					.minus(VectorUtil.parseDense(vector))
					.normL1() < 1e-10
			);
		}
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING()});

		DCTMapper dctMapper = new DCTMapper(schema, new Params().set(DCTParams.SELECTED_COL, "vec"));

		DCTMapper inverseDCTMapper = new DCTMapper(schema,
			new Params().set(DCTParams.SELECTED_COL, "vec").set(DCTParams.INVERSE, true));

		Random generator = new Random(1234);
		int data_num = 10;
		int col_num = 31;
		Row[] rows = new Row[data_num];
		for (int index = 0; index < data_num; index++) {
			double[] cur_double = new double[col_num];
			for (int index2 = 0; index2 < col_num; index2++) {
				cur_double[index2] = ((int) (generator.nextDouble() * 512) - 256) * 1.0;
			}
			rows[index] = Row.of(VectorUtil.toString(new DenseVector(cur_double)));
		}

		for (Row row : rows) {
			assertTrue(
				VectorUtil.parseDense((String) inverseDCTMapper.map(dctMapper.map(row)).getField(0))
					.minus(VectorUtil.parseDense((String) row.getField(0)))
					.normL1() < 1e-10
			);
		}

	}

}