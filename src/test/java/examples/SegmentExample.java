package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.nlp.Segment;
import com.alibaba.alink.pipeline.nlp.StopWordsRemover;
import org.junit.Test;

/**
 * Example for Segment.
 */
public class SegmentExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {1, "别人复习是查漏补缺"}),
			Row.of(new Object[] {2, "权限平台是阿里基于10余年权限管理与维护经验完全自主研发的权限管理平台"}),
			Row.of(new Object[] {3, "我们辅助用户简单快速低成本低风险的实现系统权限安全管理"})
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence"});

		Segment segment = new Segment()
			.setSelectedCol("sentence");

		StopWordsRemover stopWordsRemover = new StopWordsRemover()
			.setSelectedCol("sentence");

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, stopWordsRemover.transform(segment.transform(data))).print();

	}
}
