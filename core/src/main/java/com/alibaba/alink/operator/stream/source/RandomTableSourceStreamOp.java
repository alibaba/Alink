package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.dataproc.RandomTable;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.RandomTableSourceStreamParams;

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
@IoOpAnnotation(name = "random_table", ioType = IOType.SourceStream)
public final class RandomTableSourceStreamOp extends BaseSourceStreamOp <RandomTableSourceStreamOp>
	implements RandomTableSourceStreamParams <RandomTableSourceStreamOp> {

	private static final long serialVersionUID = 7610753144369389099L;

	public RandomTableSourceStreamOp() {
		this(new Params());
	}

	public RandomTableSourceStreamOp(long maxRows, int numCols) {
		this(new Params().set(MAX_ROWS, maxRows)
			.set(NUM_COLS, numCols));
	}

	public RandomTableSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(RandomTableSourceStreamOp.class), params);
	}

	/**
	 * example of confs string : 'col0:uniform(0,1,nullper=0.1);col1:uniform_open(0,1)'
	 */
	Map <String, Tuple3 <String, Double[], Double>> parseColConfs(String confString, String[] colNames) {
		Map <String, Tuple3 <String, Double[], Double>> confs = new HashMap <>(0);
		if (confString != null) {
			String[] items = confString.split(";");
			for (String conf : items) {
				int idx = conf.indexOf(':');
				String colName = conf.substring(0, idx);
				String distInfo = conf.substring(idx + 1, conf.length());
				String method = distInfo.substring(0, distInfo.indexOf("(")).trim();

				String val = distInfo.substring(distInfo.indexOf("(") + 1, distInfo.indexOf(")"));

				String[] vals = val.split(",");

				if (("uniform").equals(method) || "uniform_open".equals(method)
					|| "gauss".equals(method) || "weight_set".equals(method)) {
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
				} else if ("poisson".equals(method)) {
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

	@Override
	public Table initializeDataSource() {

		long maxRows = getMaxRows();
		int numCols = getNumCols();
		String idColName = getIdCol();
		String[] colNames = getOutputCols();
		String colConfsString = getOutputColConfs();
		Double timePerSample = getTimePerSample();
		Double[] timeZones = getTimeZones();
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
		StreamOperator<?> initData;
		if (timePerSample != null && idColName != null) {
			initData = new NumSeqSourceStreamOp(1, maxRows, idColName, timePerSample, getParams());
		} else if (timePerSample != null && idColName == null) {
			initData = new NumSeqSourceStreamOp(1, maxRows, timePerSample, getParams());
		} else if (timePerSample == null && timeZones == null && idColName != null) {
			initData = new NumSeqSourceStreamOp(1, maxRows, idColName, getParams());
		} else if (timeZones != null && idColName == null) {
			initData = new NumSeqSourceStreamOp(1, maxRows, timeZones, getParams());
		} else if (timeZones != null && idColName != null) {
			initData = new NumSeqSourceStreamOp(1, maxRows, idColName, timeZones, getParams());
		} else {
			initData = new NumSeqSourceStreamOp(1, maxRows, getParams());
		}

		return initData
			.udtf(idColName, colNames,
				new RandomTable(colConfs, colNames), keepColNames)
			.getOutputTable();
	}
}
