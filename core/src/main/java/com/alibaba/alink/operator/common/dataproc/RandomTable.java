package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.probabilistic.XRandom;

import java.util.Map;

/**
 * Random operation for RandomTableSource.
 */
public class RandomTable extends TableFunction <Row> {

	private static final long serialVersionUID = -3956054864602565059L;
	private Map <String, Tuple3 <String, Double[], Double>> confs;
	private XRandom rd = new XRandom();
	private String[] colNames;
	private boolean update_seed = false;

	public RandomTable(Map <String, Tuple3 <String, Double[], Double>> confs, String[] colNames) {
		this.confs = confs;
		this.colNames = colNames;

		for (String name : colNames) {

			Tuple3 <String, Double[], Double> conf = confs.get(name);

			if (conf.f0.equals("weight_set")) {
				int size = conf.f1.length / 2;
				double[] weight = new double[size];

				for (int i = 0; i < size; ++i) {
					weight[i] = conf.f1[2 * i + 1];
				}

				double sum = 0;
				for (int i = 0; i < size; i++) {
					sum += weight[i];
				}
				for (int i = 0; i < size; i++) {
					weight[i] /= sum;
				}
				for (int i = 1; i < size; i++) {
					weight[i] += weight[i - 1];
				}

				for (int i = 0; i < size; i++) {
					conf.f1[2 * i + 1] = weight[i];
				}
			}
		}

	}

	public void eval(Long idx) {
		Row row = new Row(colNames.length);
		if (!update_seed) {
			rd.setSeed(idx);
			update_seed = true;
		}
		int iter = 0;
		for (String name : colNames) {
			Tuple3 <String, Double[], Double> conf = confs.get(name);

			if (conf.f0.equals("uniform")) {
				if (conf.f2 <= 0.0) {
					row.setField(iter++, rd.uniformDistArray(1, conf.f1[0], conf.f1[1])[0]);
				} else {
					double judge = rd.uniformDistArray(1, 0.0, 1.0)[0];
					if (judge > conf.f2) {
						row.setField(iter++, rd.uniformDistArray(1, conf.f1[0], conf.f1[1])[0]);
					} else {
						row.setField(iter++, null);
					}
				}
			} else if (conf.f0.equals("uniform_open")) {
				if (conf.f2 <= 0.0) {
					double val = rd.uniformDistArray_OpenInterval(1)[0];
					row.setField(iter++, conf.f1[0] + val * (conf.f1[1] - conf.f1[0]));
				} else {
					double judge = rd.uniformDistArray(1, 0.0, 1.0)[0];
					if (judge > conf.f2) {
						double val = rd.uniformDistArray_OpenInterval(1)[0];
						row.setField(iter++, conf.f1[0] + val * (conf.f1[1] - conf.f1[0]));
					} else {
						row.setField(iter++, null);
					}
				}
			} else if (conf.f0.equals("gauss")) {
				if (conf.f2 <= 0.0) {
					row.setField(iter++, rd.normalDistArray(1, conf.f1[0], conf.f1[1])[0]);
				} else {
					double judge = rd.uniformDistArray(1, 0.0, 1.0)[0];
					if (judge > conf.f2) {
						row.setField(iter++, rd.normalDistArray(1, conf.f1[0], conf.f1[1])[0]);
					} else {
						row.setField(iter++, null);
					}
				}
			} else if (conf.f0.equals("weight_set")) {
				if (conf.f2 <= 0.0) {
					double weight_rand = rd.uniformDistArray(1, 0.0, 1.0)[0];
					int j = 0;
					for (int i = 0; i < conf.f1.length / 2; ++i) {
						if (weight_rand > conf.f1[2 * i + 1]) {
							j++;
						}
					}
					row.setField(iter++, conf.f1[2 * j]);
				} else {
					double judge = rd.uniformDistArray(1, 0.0, 1.0)[0];
					if (judge > conf.f2) {
						double weight_rand = rd.uniformDistArray(1, 0.0, 1.0)[0];
						int j = 0;
						for (int i = 0; i < conf.f1.length / 2; ++i) {
							if (weight_rand > conf.f1[2 * i + 1]) {
								j++;
							}
						}
						row.setField(iter++, conf.f1[2 * j]);
					} else {
						row.setField(iter++, null);
					}
				}
			} else if (conf.f0.equals("poisson")) {
				if (conf.f2 <= 0.0) {
					row.setField(iter++, (double) (rd.poisonDistArray(1, conf.f1[0])[0]));
				} else {
					double judge = rd.uniformDistArray(1, 0.0, 1.0)[0];
					if (judge > conf.f2) {
						row.setField(iter++, (double) (rd.poisonDistArray(1, conf.f1[0])[0]));
					} else {
						row.setField(iter++, null);
					}
				}

			} else {
				throw (new AkUnsupportedOperationException("not support this distribution."));
			}
		}

		collect(row);
		return;
	}

	@Override
	public TypeInformation <Row> getResultType() {
		TypeInformation[] types = new TypeInformation[this.colNames.length];
		for (int i = 0; i < this.colNames.length; ++i) {
			types[i] = Types.DOUBLE;
		}
		return new RowTypeInfo(types);
	}
}
