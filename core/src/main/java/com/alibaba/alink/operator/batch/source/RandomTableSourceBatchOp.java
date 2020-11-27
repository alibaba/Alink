package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.RandomTable;
import com.alibaba.alink.params.io.RandomTableSourceBatchParams;

import java.util.HashMap;
import java.util.Map;

/**
 * Generate table with random data.
 * We support five random type of each column: uniform, uniform_open, gauss, weight_set and poisson.
 * uniform(1,2,nullper=0.1): uniform from 1 to 2 with 0.1 of the data is null;
 * uniform_open(1,2): uniform from 1 to 2 in the open space;
 * weight_set(1.0,3.0,2.0,5.0): random generate data of 1.0 and 2.0 while the ratio of the two is 1:2;
 * gauss(0,1): generate data from gauss(0, 1);
 * poisson(0.5): generate data from poisson distribution with lambda = 0.5 .
 */
@IoOpAnnotation(name = "random_table", ioType = IOType.SourceBatch)
public final class RandomTableSourceBatchOp extends BaseSourceBatchOp <RandomTableSourceBatchOp>
	implements RandomTableSourceBatchParams <RandomTableSourceBatchOp> {

	private static final long serialVersionUID = -6171191677350106691L;

	public RandomTableSourceBatchOp() {
		this(new Params());
	}

	public RandomTableSourceBatchOp(long numRows, int numCols) {
		this(new Params().set(NUM_ROWS, numRows)
			.set(NUM_COLS, numCols));
	}

	// params : colNamesString example "f0,f1,f2,f3,f4,f5" //todo : keep array
	// params : colConfsString example "f0:uniform(0,1,nullper=0.1);f1:uniform_open(0,1)"
	public RandomTableSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(RandomTableSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		long numRows = getNumRows();
		int numCols = getNumCols();
		String idColName = getIdCol();
		String[] colNames = getOutputCols();
		String colConfsString = getOutputColConfs();

		String[] keepColNames = idColName != null ? new String[] {idColName} : new String[] {};

		if (colNames == null) {
			colNames = new String[numCols];
			for (int i = 0; i < numCols; ++i) {
				colNames[i] = "col" + i;
			}
		}

		for (int i = 0; i < colNames.length; i++) {
			colNames[i] = colNames[i].trim();
		}

		Map <String, Tuple3 <String, Double[], Double>> colConfs = parseColConfs(colConfsString, colNames);

		BatchOperator<?> initData = idColName != null ? new NumSeqSourceBatchOp(0, numRows - 1, idColName, getParams())
			: new NumSeqSourceBatchOp(0L, numRows - 1, getParams());
		if (idColName == null) { idColName = "num"; }
		return initData
			.udtf(idColName, colNames,
				new RandomTable(colConfs, colNames), keepColNames)
			.getOutputTable();
	}

	// example of confs string : 'col0:uniform(0,1,nullper=0.1);col1:uniform_open(0,1)'
	Map <String, Tuple3 <String, Double[], Double>> parseColConfs(String confString, String[] colNames) {
		Map <String, Tuple3 <String, Double[], Double>> confs = new HashMap <>();
		if (confString != null) {
			String[] items = confString.split(";");
			for (String conf : items) {
				int idx = conf.indexOf(':');
				String colName = conf.substring(0, idx);
				String distInfo = conf.substring(idx + 1, conf.length());
				String method = distInfo.substring(0, distInfo.indexOf("(")).trim();

				String val = distInfo.substring(distInfo.indexOf("(") + 1, distInfo.indexOf(")"));

				String[] vals = val.split(",");

				if (method.equals("uniform") || method.equals("uniform_open")
					|| method.equals("gauss") || method.equals("weight_set")) {
					if (vals.length % 2 == 0) {
						Double[] values = new Double[vals.length];
						for (int i = 0; i < vals.length; ++i) {
							values[i] = Double.parseDouble(vals[i]);
						}
						confs.put(colName, Tuple3.of(method, values, -1.0));
					} else {
						Double[] values = new Double[vals.length - 1];
						for (int i = 0; i < vals.length - 1; ++i) {
							values[i] = Double.parseDouble(vals[i]);
						}
						String str = vals[vals.length - 1];
						Double nullper = Double.parseDouble(str.substring(str.indexOf("=") + 1, str.length()));
						confs.put(colName, Tuple3.of(method, values, nullper));
					}
				} else if (method.equals("poisson")) {
					if (vals.length == 1) {
						Double[] values = new Double[vals.length];
						for (int i = 0; i < vals.length; ++i) {
							values[i] = Double.parseDouble(vals[i]);
						}
						confs.put(colName, Tuple3.of(method, values, -1.0));
					} else if (vals.length == 2) {
						Double[] values = new Double[vals.length - 1];
						for (int i = 0; i < vals.length - 1; ++i) {
							values[i] = Double.parseDouble(vals[i]);
						}
						String str = vals[vals.length - 1];
						Double nullper = Double.parseDouble(str.substring(str.indexOf("=") + 1, str.length() - 1));
						confs.put(colName, Tuple3.of(method, values, nullper));
					} else {
						throw (new RuntimeException("poisson distribution parameter error."));
					}
				}
			}
		}
		for (String name : colNames) {
			if (!confs.containsKey(name)) {
				confs.put(name, Tuple3.of("uniform", new Double[] {0.0, 1.0}, -1.0));
			}
		}
		return confs;
	}
}
