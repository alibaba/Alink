package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;

import java.io.File;

public class Utils {

	public static final String ROOT_DIR = "/Users/yangxu/alink/data/";

	public static String generateSchemaString(String[] colNames, String[] colTypes) {
		int n = colNames.length;
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < n; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append(colNames[i]).append(" ").append(colTypes[i]);
		}

		return sbd.toString();
	}

	public static void splitTrainTestIfNotExist(
		BatchOperator <?> source,
		String trainFilePath,
		String testFilePath,
		double ratio
	) throws Exception {
		if ((!new File(trainFilePath).exists()) && (!new File(testFilePath).exists())) {
			SplitBatchOp spliter = new SplitBatchOp().setFraction(ratio);

			source.link(spliter);

			spliter
				.link(
					new AkSinkBatchOp()
						.setFilePath(trainFilePath)
				);

			spliter.getSideOutput(0)
				.link(
					new AkSinkBatchOp()
						.setFilePath(testFilePath)
				);

			BatchOperator.execute();
		}
	}

}
