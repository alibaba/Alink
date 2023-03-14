package com.alibaba.alink.operator.common.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.exceptions.AkParseErrorException;

import java.util.HashMap;
import java.util.Map;

public class RandomTableSourceUtils {

	// example of confs string : 'col0:uniform(0,1,nullper=0.1);col1:uniform_open(0,1)'
	public static Map <String, Tuple3 <String, Double[], Double>> parseColConfs(String confString, String[] colNames) {
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
						throw new AkParseErrorException("cannot parse poisson distribution parameter ");
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
