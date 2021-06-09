package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.nlp.TokenizerParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for TokenizerMapper.
 */

public class TokenizerMapperTest extends AlinkTestBase {
	@Test
	public void testDefault() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"sentence", "id"},
			new TypeInformation <?>[] {Types.STRING, Types.INT});

		Params params = new Params()
			.set(TokenizerParams.SELECTED_COL, "sentence");

		TokenizerMapper mapper = new TokenizerMapper(schema, params);
		mapper.open();
		assertEquals(mapper.map(Row.of("This\tis  a unit test for mapper", 1)).getField(0),
			"this is a unit test for mapper");
		assertEquals(mapper.map(Row.of(null, 2)).getField(0), null);

		assertEquals(mapper.getOutputSchema(), schema);
		mapper.close();
	}

}
