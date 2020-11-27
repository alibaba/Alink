package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.dataproc.KeyToValueParams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KeyToValueModelMapperTest {
	@Test
	public void map() throws Exception {
		TableSchema modelSchema = new TableSchema(new String[] {"key_col", "value_col"},
			new TypeInformation[] {Types.INT, Types.STRING});

		Row[] rows = new Row[] {
			Row.of(-3, "neg3"),
			Row.of(10, "pos10")
		};

		List <Row> model = Arrays.asList(rows);

		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1"},
			new TypeInformation <?>[] {Types.INT, Types.DOUBLE}
		);
		Params params = new Params()
			.set(KeyToValueParams.MAP_KEY_COL, "key_col")
			.set(KeyToValueParams.MAP_VALUE_COL, "value_col")
			.set(KeyToValueParams.SELECTED_COL, "f0");

		KeyToValueModelMapper mapper = new KeyToValueModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals((String) mapper.map(Row.of(10, 2.0)).getField(0), "pos10");
		assertEquals((String) mapper.map(Row.of(1, 2.0)).getField(0), null);
		assertEquals((String) mapper.map(Row.of(-3, 2.0)).getField(0), "neg3");
	}

}