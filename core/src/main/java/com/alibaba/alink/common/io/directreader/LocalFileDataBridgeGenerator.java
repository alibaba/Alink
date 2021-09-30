package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvFormatter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

@DataBridgeGeneratorPolicy(policy = "local_file")
public class LocalFileDataBridgeGenerator implements DataBridgeGenerator {
	@Override
	public DataBridge generate(BatchOperator <?> batchOperator, Params params) {
		TypeInformation <?>[] colTypes = batchOperator.getColTypes();
		File file;
		try {
			file = File.createTempFile("alink-data-bridge-", ".ak");
			Runtime.getRuntime().addShutdownHook(new Thread(() -> file.delete()));
		} catch (IOException e) {
			throw new RuntimeException("Cannot create temp file.");
		}
		//new AkSinkBatchOp()
		//	.setFilePath(file.getAbsolutePath())
		//	.setOverwriteSink(true)
		//	.linkFrom(batchOperator);
		//try {
		//	BatchOperator.execute();
		//} catch (Exception e) {
		//	throw new RuntimeException(e);
		//}
		List <Row> rows;
		try {
			rows = batchOperator.getDataSet().collect();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		CsvFormatter formatter = new CsvFormatter(colTypes, ",", '"');
		try (FileOutputStream fos = new FileOutputStream(file)) {
			PrintWriter writer = new PrintWriter(fos);
			for (Row row : rows) {
				String s = formatter.format(row);
				writer.println(s);
			}
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		int size = rows.size();
		rows.clear();
		System.gc();
		return new LocalFileDataBridge(file.getAbsolutePath(), size, colTypes);
	}
}
