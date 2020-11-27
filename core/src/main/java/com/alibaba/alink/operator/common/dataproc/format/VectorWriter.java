package com.alibaba.alink.operator.common.dataproc.format;

import com.alibaba.alink.common.linalg.SparseVector;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.Map;

public class VectorWriter extends FormatWriter {

	private static final long serialVersionUID = 125456403779259536L;
	private final long size;
	private final String[] colNames;

	public VectorWriter(long size, String[] colNames) {
		this.size = size;
		this.colNames = colNames;
	}

	@Override
	public Tuple2 <Boolean, Row> write(Map <String, String> in) {
		if (null == this.colNames) {
			int itemSize = in.size();
			int[] indices = new int[itemSize];
			double[] values = new double[itemSize];
			int count = 0;
			for (Map.Entry <String, String> entry : in.entrySet()) {
				if (!NumberUtils.isDigits(entry.getKey())) {
					return Tuple2.of(false, new Row(0));
				}
				indices[count] = Integer.parseInt(entry.getKey());
				if (!NumberUtils.isNumber(entry.getValue())) {
					return Tuple2.of(false, new Row(0));
				}
				values[count] = Double.parseDouble(entry.getValue());
				count++;
			}

			return new Tuple2 <>(true, Row.of(new SparseVector((int) this.size, indices, values).toString()));

		} else {
			StringBuilder sbd = new StringBuilder();

			int n = colNames.length;
			if (this.size > colNames.length) {
				sbd.append("$").append(this.size).append("$");
			} else if (this.size > 0 && this.size < colNames.length) {
				n = (int) this.size;
			}

			for (int i = 0; i < n; i++) {
				if (i > 0) {
					sbd.append(" ");
				}

				String v = in.get(colNames[i]);
				if (!NumberUtils.isNumber(v)) {
					return Tuple2.of(false, new Row(0));
				}
				sbd.append(v);
			}

			return new Tuple2 <>(true, Row.of(sbd.toString()));

		}
	}
}
