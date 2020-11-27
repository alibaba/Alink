package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.probabilistic.XRandom;

import java.util.Set;
import java.util.TreeSet;

/**
 * Random operation for RandomVectorSource.
 */
public class RandomVector extends TableFunction <Row> {

	private static final long serialVersionUID = 3454599342587984872L;
	private XRandom rd = new XRandom();
	private Integer[] size;
	private int row_size;
	private int nonZeroPerRow;
	private double sparsity;

	private boolean update_seed = false;

	public RandomVector(Integer[] size, double sparsity) {
		this.size = size;
		this.row_size = 1;
		for (int i = 0; i < size.length; ++i) {
			row_size *= size[i];
		}
		this.sparsity = sparsity;
	}

	public void eval(Long idx) {
		if (!update_seed) {
			rd.setSeed(idx);
			update_seed = true;
		}
		Row row = new Row(1);
		double off = rd.nextDouble();
		//System.out.println(off);
		nonZeroPerRow = Math.min(row_size, (int) (row_size * sparsity * (1 - 0.3 + 0.6 * off)));

		Set <Integer> set = new TreeSet <>();

		for (int i = 0; i < nonZeroPerRow; ++i) {
			set.add(rd.nextInt(row_size));
		}

		StringBuilder sb = new StringBuilder();
		sb.append("$" + size[0]);
		for (int i = 1; i < size.length; ++i) {
			sb.append(" " + size[i]);
		}
		sb.append("$");

		for (Integer ele : set) {
			int[] pos = new int[this.size.length];
			int tmp_size = row_size;
			int remain_size = ele;
			for (int i = 0; i < this.size.length; ++i) {
				tmp_size = tmp_size / this.size[i];
				pos[i] = remain_size / tmp_size;
				remain_size = remain_size - pos[i] * tmp_size;
			}

			for (int i = 0; i < this.size.length; ++i) {
				sb.append(pos[i] + ":");
			}
			sb.append(rd.nextDouble() + " ");
		}
		if (sb.indexOf("$", 1) != sb.length() - 1) {
			sb.delete(sb.length() - 1, sb.length());
		}
		row.setField(0, sb.toString());
		collect(row);
		return;
	}

	@Override
	public TypeInformation <Row> getResultType() {
		TypeInformation[] types = new TypeInformation[1];
		for (int i = 0; i < 1; ++i) {
			types[i] = Types.STRING;
		}
		return new RowTypeInfo(types);
	}
}
