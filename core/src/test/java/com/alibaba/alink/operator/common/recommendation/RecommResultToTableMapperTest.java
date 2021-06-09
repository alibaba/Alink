package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.params.recommendation.FlattenKObjectParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for RecommResultToTableMapper.
 */

public class RecommResultToTableMapperTest extends AlinkTestBase {
	private static Row[] rows = new Row[] {
		Row.of(3L, "{\"rating\":\"[0.6,0.1]\",\"recomm\":\"[1,3]\"}", "{\"calc\":\"[0.11404907704943987,"
			+ "0.17940763151627642,0.26367136145632203]\",\"recomm\":\"[\\\"1\\\",\\\"2\\\",\\\"3\\\"]\"}"),
		Row.of(2L, "{\"rating\":\"[0.6]\",\"recomm\":\"[3]\"}", "{\"calc\":\"[0.0904194430179465,"
			+ "0.19730387943005356,0.33907291131729933]\",\"recomm\":\"[\\\"2\\\",\\\"1\\\",\\\"3\\\"]\"}\n")
	};
	private static TableSchema dataSchema = new TableSchema(new String[] {"user", "label", "recomm"},
		new TypeInformation[] {
			Types.LONG, Types.STRING, Types.STRING
		});

	@Test
	public void flatMap() {
		Params params = new Params()
			.set(FlattenKObjectParams.SELECTED_COL, "recomm")
			.set(FlattenKObjectParams.OUTPUT_COLS, new String[] {"calc"})
			.set(FlattenKObjectParams.OUTPUT_COL_TYPES, new String[] {"long"});

		FlattenKObjectMapper mapper = new FlattenKObjectMapper(dataSchema, params);
		RowCollector collector = new RowCollector();
		for (Row row : rows) {
			mapper.flatMap(row, collector);
		}
		Assert.assertEquals(collector.getRows().size(), 6);
	}
}