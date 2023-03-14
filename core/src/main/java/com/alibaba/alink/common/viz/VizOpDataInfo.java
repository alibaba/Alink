package com.alibaba.alink.common.viz;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

public class VizOpDataInfo implements AlinkSerializable, Serializable {

	public int dataId;
	public String dataType; //table, kv
	public String colNames = null;//col1,col2,col3, only for table
	public String writeDataType = "continuous"; //continuous or onlyOnce

	public VizOpDataInfo(int dataId) {
		this.dataId = dataId;
		this.dataType = "kv";
	}

	public VizOpDataInfo(int dataId, String colNames) {
		this.dataId = dataId;
		this.colNames = colNames;
		this.dataType = "table";
	}

	public VizOpDataInfo(int dataId, WriteVizDataType writeDataType) {
		this.dataId = dataId;
		this.dataType = "kv";
		if (writeDataType == WriteVizDataType.OnlyOnce) {
			this.writeDataType = "onlyOnce";
		}
	}

	public VizOpDataInfo(int dataId, String colNames, WriteVizDataType writeDataType) {
		this.dataId = dataId;
		this.colNames = colNames;
		this.dataType = "table";
		if (writeDataType == WriteVizDataType.OnlyOnce) {
			this.writeDataType = "onlyOnce";
		}
	}

	public enum WriteVizDataType {
		Continuous,
		OnlyOnce
	}
}
