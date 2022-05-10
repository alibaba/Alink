package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ToMTableBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.ToMTableParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ToMTableMapperTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {

		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "2", 0, new Timestamp(20000031232012L),
			null,
			new SparseVector(3, new int[] {1}, new double[] {2.0}),
			new FloatTensor(new float[] {3.0f})));

		MTable mTable = new MTable(
			rows,
			"col0 int, col1 string, label int, ts timestamp"
				+ ", d_vec DENSE_VECTOR"
				+ ", s_vec VECTOR"
				+ ", tensor FLOAT_TENSOR");

		final Mapper mapper = new ToMTableMapper(
			new TableSchema(
				new String[] {"mTable"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToMTableParams.SELECTED_COL, "mTable")
		);

		MTable result = (MTable) mapper.map(Row.of(JsonConverter.toJson(mTable))).getField(0);
		Assert.assertEquals(mTable.toString(), result.toString());
		System.out.println(mTable.toString());
	}

	@Test
	public void testHandleInvalidSkip() throws Exception {
		Mapper mapper = new ToMTableMapper(
			new TableSchema(
				new String[] {"mTable"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToMTableParams.SELECTED_COL, "mTable")
				.set(ToMTableParams.HANDLE_INVALID, HandleInvalidMethod.SKIP)
		);

		String vecStr = "df as df as df da sf";
		final MTable result = (MTable) mapper.map(Row.of(vecStr)).getField(0);
		Assert.assertNull(result);
	}

	@Test
	public void testOp() throws Exception {
		final String mTableStr = "{\"data\":{\"col0\":[1],\"col1\":[\"2\"],\"label\":[0],\"ts\":[\"2603-10-12 04:13:52"
			+ ".012\"],\"d_vec\":[null],\"s_vec\":[\"$3$1:2.0\"],\"tensor\":[\"FLOAT#1#3.0 \"]},\"schema\":\"col0 INT,"
			+ "col1 VARCHAR,label INT,ts TIMESTAMP,d_vec DENSE_VECTOR,s_vec VECTOR,tensor FLOAT_TENSOR\"}";
		final MTable expect = MTable.fromJson(mTableStr);

		Row[] rows = new Row[] {
			Row.of(mTableStr)
		};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			rows, new String[] {"mTable"}
		);

		memSourceBatchOp
			.link(
				new ToMTableBatchOp()
					.setSelectedCol("mTable")
			)
			.lazyCollect(rows1 -> Assert.assertEquals(expect.toString(), rows1.get(0).getField(0).toString()));

		BatchOperator.execute();
	}
}