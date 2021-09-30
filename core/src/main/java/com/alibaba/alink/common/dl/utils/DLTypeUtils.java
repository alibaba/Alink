package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.TypeConvertBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.TypeConvertStreamOp;
import com.alibaba.alink.params.dataproc.HasTargetType.TargetType;

import java.util.ArrayList;
import java.util.List;

public class DLTypeUtils {
	public static BatchOperator <?> doubleColumnsToFloat(BatchOperator <?> input) {
		List <String> doubleColNames = new ArrayList <>();

		String[] colNames = input.getColNames();
		TypeInformation <?>[] colTypes = input.getColTypes();
		for (int i = 0; i < colTypes.length; i += 1) {
			if (colTypes[i].equals(Types.DOUBLE)) {
				doubleColNames.add(colNames[i]);
			}
		}

		if (doubleColNames.size() > 0) {
			TypeConvertBatchOp typeConvertBatchOp = new TypeConvertBatchOp()
				.setTargetType(TargetType.FLOAT)
				.setSelectedCols(doubleColNames.toArray(new String[0]))
				.setMLEnvironmentId(input.getMLEnvironmentId());
			input = typeConvertBatchOp.linkFrom(input);
		}
		return input;
	}

	public static StreamOperator <?> doubleColumnsToFloat(StreamOperator <?> input) {
		List <String> doubleColNames = new ArrayList <>();

		String[] colNames = input.getColNames();
		TypeInformation <?>[] colTypes = input.getColTypes();
		for (int i = 0; i < colTypes.length; i += 1) {
			if (colTypes[i].equals(Types.DOUBLE)) {
				doubleColNames.add(colNames[i]);
			}
		}

		if (doubleColNames.size() > 0) {
			TypeConvertStreamOp typeConvertStreamOp = new TypeConvertStreamOp()
				.setTargetType(TargetType.FLOAT)
				.setSelectedCols(doubleColNames.toArray(new String[0]))
				.setMLEnvironmentId(input.getMLEnvironmentId());
			input = typeConvertStreamOp.linkFrom(input);
		}
		return input;
	}
}
