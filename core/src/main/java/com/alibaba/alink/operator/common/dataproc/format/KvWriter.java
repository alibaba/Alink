package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;

import java.util.Map;

public class KvWriter extends FormatWriter {

	private static final long serialVersionUID = -5194546782659446514L;
	final String colDelimiter;
	final String valDelimiter;

	public KvWriter(String colDelimiter, String valDelimiter) {
		this.colDelimiter = colDelimiter;
		this.valDelimiter = valDelimiter;
	}

	@Override
	public Tuple2 <Boolean, Row> write(Map <String, String> in) {
		StringBuilder sbd = new StringBuilder();

		boolean isFirstPair = true;
		if (valDelimiter != null) {
			for (Map.Entry entry : in.entrySet()) {
				if (isFirstPair) {
					isFirstPair = false;
				} else {
					sbd.append(colDelimiter);
				}
				sbd.append(entry.getKey() + valDelimiter + entry.getValue());
			}
		} else {
			int itemSize = in.size();
			int[] indices = new int[itemSize];
			double[] values = new double[itemSize];
			int count = 0;
			for (Map.Entry <String, String> entry : in.entrySet()) {
				indices[count] = ((Double) Double.parseDouble(entry.getKey())).intValue();
				values[count] = Double.parseDouble(entry.getValue());
				count++;
			}
			values = new SparseVector(-1, indices, values).getValues();
			for (double value : values) {
				if (isFirstPair) {
					isFirstPair = false;
				} else {
					sbd.append(colDelimiter);
				}
				sbd.append(value);
			}
		}

		return new Tuple2 <>(true, Row.of(sbd.toString()));
	}
}
