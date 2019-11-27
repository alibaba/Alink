package examples;

import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.pipeline.dataproc.MaxAbsScaler;
import com.alibaba.alink.pipeline.dataproc.MaxAbsScalerModel;
import com.alibaba.alink.pipeline.dataproc.GenerateData;
import org.junit.Test;

public class MaxAbsScalerExample {
	private static void testPipelineI() throws Exception {

		//////////////// table //////////////
		Table tableSource = GenerateData.getBatchTable();
		Table sTableSource = GenerateData.getStreamTable();

		String[] selectedColNames = new String[] {"f0", "f1"};

		MaxAbsScaler scaler3 = new MaxAbsScaler()
			.setSelectedCols(selectedColNames);

		MaxAbsScalerModel denseModel3 = scaler3.fit(tableSource);
		TestUtil.printTable(denseModel3.transform(tableSource));
		TestUtil.printTable(denseModel3.transform(sTableSource));

	}

	@Test
	public void testPipeline() throws Exception {
		testPipelineI();
	}
}
