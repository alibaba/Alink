package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.nlp.SegmentParams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SegmentMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation <?>[] {Types.STRING()});

		Params params = new Params()
			.set(SegmentParams.SELECTED_COL, "sentence");

		SegmentMapper mapper = new SegmentMapper(schema, params);

		assertEquals(mapper.map(Row.of("我们辅助用户简单快速低成本低风险的实现系统权限安全管理")).getField(0),
			"我们 辅助 用户 简单 快速 低成本 低 风险 的 实现 系统 权限 安全 管理");
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"sentence"}, new TypeInformation <?>[] {Types.STRING()});
		String[] dictArray = new String[] {"低风险"};

		Params params = new Params()
			.set(SegmentParams.SELECTED_COL, "sentence")
			.set(SegmentParams.USER_DEFINED_DICT, dictArray);

		SegmentMapper mapper = new SegmentMapper(schema, params);

		assertEquals(mapper.map(Row.of("我们辅助用户简单快速低成本低风险的实现系统权限安全管理")).getField(0),
			"我们 辅助 用户 简单 快速 低成本 低风险 的 实现 系统 权限 安全 管理");
		assertEquals(mapper.getOutputSchema(), schema);
	}
}
