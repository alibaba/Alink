package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorType;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ToVectorBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.ToVectorParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class ToVectorMapperTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		final Mapper mapper = new ToVectorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToVectorParams.SELECTED_COL, "vec")
		);

		String vecStr = "0.0 0.1 1.0 1.1 2.0 2.1";
		DenseVector vector = VectorUtil.parseDense(vecStr);
		DenseVector result = (DenseVector) mapper.map(Row.of(vecStr)).getField(0);
		Assert.assertEquals(vector, result);

		String sVecStr = "0:0.0 1:0.1 5:1.0 10:1.1 12:2.0 15:2.1";
		SparseVector sVector = VectorUtil.parseSparse(sVecStr);
		SparseVector sResult = (SparseVector) mapper.map(Row.of(sVecStr)).getField(0);
		Assert.assertEquals(sVector, sResult);
	}

	@Test
	public void testVec() throws Exception {
		final Mapper mapper = new ToVectorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {VectorTypes.VECTOR}
			),
			new Params()
				.set(ToVectorParams.SELECTED_COL, "vec")
		);

		String vecStr = "0.0 0.1 1.0 1.1 2.0 2.1";
		DenseVector vector = VectorUtil.parseDense(vecStr);
		DenseVector result = (DenseVector) mapper.map(Row.of(vector)).getField(0);
		Assert.assertEquals(vector, result);

		String sVecStr = "0:0.0 1:0.1 5:1.0 10:1.1 12:2.0 15:2.1";
		SparseVector sVector = VectorUtil.parseSparse(sVecStr);
		SparseVector sResult = (SparseVector) mapper.map(Row.of(sVector)).getField(0);
		Assert.assertEquals(sVector, sResult);
	}

	@Test
	public void testDefaultType() {
		Mapper mapper = new ToVectorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToVectorParams.VECTOR_TYPE, VectorType.DENSE)
				.set(ToVectorParams.SELECTED_COL, "vec")
		);
		Assert.assertEquals(VectorTypes.DENSE_VECTOR, mapper.getOutputSchema().getFieldTypes()[0]);

		mapper = new ToVectorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToVectorParams.VECTOR_TYPE, VectorType.SPARSE)
				.set(ToVectorParams.SELECTED_COL, "vec")
		);
		Assert.assertEquals(VectorTypes.SPARSE_VECTOR, mapper.getOutputSchema().getFieldTypes()[0]);

		mapper = new ToVectorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToVectorParams.SELECTED_COL, "vec")
		);
		Assert.assertEquals(VectorTypes.VECTOR, mapper.getOutputSchema().getFieldTypes()[0]);
	}

	@Test
	public void testHandleInvalidSkip() throws Exception {
		Mapper mapper = new ToVectorMapper(
			new TableSchema(
				new String[] {"vec"},
				new TypeInformation <?>[] {Types.STRING}
			),
			new Params()
				.set(ToVectorParams.VECTOR_TYPE, VectorType.DENSE)
				.set(ToVectorParams.SELECTED_COL, "vec")
				.set(ToVectorParams.HANDLE_INVALID, HandleInvalidMethod.SKIP)
		);

		String vecStr = "df as df as df da sf";
		final DenseVector result = (DenseVector) mapper.map(Row.of(vecStr)).getField(0);
		Assert.assertNull(result);
	}

	@Test
	public void testOp() throws Exception {
		final String vecStr = "1 0 3 4";
		final DenseVector expect = VectorUtil.parseDense("1 0 3 4");
		final SparseVector expect1 = VectorUtil.parseSparse("$4$0:1 2:3 3:4");

		Row[] rows = new Row[] {
			Row.of(vecStr)
		};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			rows, new String[] {"vec"}
		);

		memSourceBatchOp
			.link(
				new ToVectorBatchOp()
					.setSelectedCol("vec")
			)
			.lazyCollect(rows1 -> Assert.assertEquals(expect, rows1.get(0).getField(0)));

		memSourceBatchOp
			.link(
				new ToVectorBatchOp()
					.setVectorType(VectorType.SPARSE)
					.setSelectedCol("vec")
			)
			.lazyCollect(rows1 -> Assert.assertEquals(expect1, rows1.get(0).getField(0)));


		BatchOperator.execute();
	}
}