package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dataproc.format.FromKvParams;
import com.alibaba.alink.params.dataproc.format.ToTripleParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AnyToTripleFlatMapperTest extends AlinkTestBase {
	@Test
	public void flatMap() throws Exception {

		AnyToTripleFlatMapper transKvToTriple = new AnyToTripleFlatMapper(
			CsvUtil.schemaStr2Schema("row_id long, kv string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.KV)
				.set(FromKvParams.KV_COL, "kv")
				.set(FromKvParams.KV_COL_DELIMITER, ",")
				.set(FromKvParams.KV_VAL_DELIMITER, ":")
				.set(ToTripleParams.TRIPLE_COLUMN_VALUE_SCHEMA_STR, "col_id int, val double")
				.set(ToTripleParams.RESERVED_COLS, new String[] {"row_id"})
		);
		transKvToTriple.open();

		RowCollector collector = new RowCollector();
		transKvToTriple.flatMap(Row.of(3L, "1:1.0,4:1.0"), collector);

		Assert.assertEquals(collector.getRows().size(), 2);
		//		for (Row row : collector.getRows()) {
		//			System.out.println(row);
		//		}

	}

}