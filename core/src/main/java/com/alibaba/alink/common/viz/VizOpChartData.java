package com.alibaba.alink.common.viz;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

public class VizOpChartData implements AlinkSerializable, Serializable {

	public int dataId;

	public String[] keys = null; //option, json value

	public VizOpChartData(int dataId) {
		this.dataId = dataId;
	}

	public VizOpChartData(int dataId, String key) {
		this.dataId = dataId;
		this.keys = new String[] {key};
	}

	public VizOpChartData(int dataId, String[] keys) {
		this.dataId = dataId;
		this.keys = keys;
	}

}
