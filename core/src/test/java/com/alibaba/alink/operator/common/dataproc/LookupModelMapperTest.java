package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.HugeLookupBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.LookupParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LookupModelMapperTest extends AlinkTestBase {
	@Test
	public void map() throws Exception {
		TableSchema modelSchema = new TableSchema(new String[] {"key_col", "value_1", "value_2"},
			new TypeInformation[] {Types.INT, Types.STRING, Types.DOUBLE});

		Row[] rows = new Row[] {
			Row.of(-3, "neg3", 2.0),
			Row.of(10, "pos10", 3.0)
		};

		List <Row> model = Arrays.asList(rows);

		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1"},
			new TypeInformation <?>[] {Types.INT, Types.DOUBLE}
		);
		Params params = new Params()
			.set(LookupParams.MAP_KEY_COLS, new String[] {"key_col"})
			.set(LookupParams.MAP_VALUE_COLS, new String[] {"value_1", "value_2"})
			.set(LookupParams.SELECTED_COLS, new String[] {"f0"});

		LookupModelMapper mapper = new LookupModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of(10, 2.0)).getField(2), "pos10");
		assertEquals(mapper.map(Row.of(10, 2.0)).getField(3), 3.0);
		assertNull(mapper.map(Row.of(1, 2.0)).getField(2));
		assertNull(mapper.map(Row.of(1, 2.0)).getField(3));
		assertEquals(mapper.map(Row.of(-3, 2.0)).getField(2), "neg3");
	}

	@Test
	public void testHugeLookup() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1"},
			new TypeInformation <?>[] {Types.INT, Types.DOUBLE}
		);

		Row[] dataRows = new Row[] {
			Row.of(10, 2.0),
			Row.of(1, 2.0),
			Row.of(-3, 2.0)
		};

		TableSchema modelSchema = new TableSchema(new String[] {"key_col", "value_1", "value_2"},
			new TypeInformation[] {Types.INT, Types.STRING, Types.DOUBLE});

		Row[] modelRows = new Row[] {
			Row.of(-3, "neg3", 2.0),
			Row.of(10, "pos10", 3.0)
		};

		BatchOperator<?> data = new MemSourceBatchOp(Arrays.asList(dataRows), dataSchema);

		BatchOperator<?> model = new MemSourceBatchOp(Arrays.asList(modelRows), modelSchema);

		new HugeLookupBatchOp()
			.setMapKeyCols("key_col")
			.setMapValueCols("value_1", "value_2")
			.setSelectedCols("f0")
			.linkFrom(model, data)
			.print();
	}
}