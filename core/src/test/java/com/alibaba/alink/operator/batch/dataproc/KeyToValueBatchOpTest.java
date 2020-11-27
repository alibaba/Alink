package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class KeyToValueBatchOpTest {

	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("0", "a", 1L, 1, 2.0, true),
				Row.of("1", null, 2L, 2, -3.0, true),
				Row.of("2", "c", null, null, 2.0, false),
				Row.of("3", "a", 0L, 0, null, null),
			};

		String[] colNames = new String[] {"id", "f_string", "f_long", "f_int", "f_double", "f_boolean"};
		TableSchema schema = new TableSchema(
			colNames,
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
		);

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), schema);
		KeyToValueBatchOp keyToValue = new KeyToValueBatchOp()
			.setSelectedCol("f_string")
			.setMapKeyCol("f_string")
			.setMapValueCol("f_double");

		keyToValue.linkFrom(source, source);
		keyToValue.print();
	}

}