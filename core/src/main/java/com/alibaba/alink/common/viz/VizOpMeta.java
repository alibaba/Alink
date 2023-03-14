package com.alibaba.alink.common.viz;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class VizOpMeta implements AlinkSerializable, Serializable {

	private static final long serialVersionUID = 3940407105766723917L;
	public String opName; //package.op AllStatBatchOp if withStat / Op

	public VizOpDataInfo[] dataInfos;
	public Map <String, VizOpChartData> cascades = new HashMap <>();

	public String opType; //stream or batch

	//screen param info
	public VizOpTableSchema[] schemas;
	public boolean isOutput = true;
	public Params params;

	public void setSchema(TableSchema s) {
		setSchemas(new TableSchema[] {s});
	}

	public void setSchema(String[] colNames, TypeInformation[] colTypes) {
		setSchemas(new TableSchema[] {new TableSchema(colNames, colTypes)});
	}

	public void setSchemas(TableSchema[] s) {
		schemas = new VizOpTableSchema[s.length];
		for (int i = 0; i < s.length; i++) {
			schemas[i] = new VizOpTableSchema();
			int len = s[i].getFieldNames().length;
			schemas[i].colNames = new String[len];
			for (int j = 0; j < len; j++) {
				schemas[i].colNames[j] = s[i].getFieldName(j).get();
			}
			schemas[i].colTypes = new String[len];
			for (int j = 0; j < len; j++) {
				schemas[i].colTypes[j] = s[i].getFieldType(j).get().toString().toUpperCase();
			}
		}

	}

	static class VizOpTableSchema implements AlinkSerializable, Serializable {
		private static final long serialVersionUID = 8371261277649665050L;
		public String[] colNames;
		public String[] colTypes;
	}
}

