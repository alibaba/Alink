package com.alibaba.alink.common.viz;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

public class VizData implements AlinkSerializable, Serializable {
	private static final long serialVersionUID = -3722895114138878009L;
	public long dataId;
	public String data;
	public long timeStamp;

	public VizData(long dataId, String data, long timeStamp) {
		this.dataId = dataId;
		this.data = data;
		this.timeStamp = timeStamp;
	}

	public long getDataId() {
		return dataId;
	}

	public String getData() {
		return data;
	}

	public long getTimeStamp() {
		return timeStamp;
	}
}
