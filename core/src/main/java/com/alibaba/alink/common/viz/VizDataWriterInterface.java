package com.alibaba.alink.common.viz;

import java.io.Serializable;
import java.util.List;

public interface VizDataWriterInterface extends Serializable {

	void writeBatchData(long dataId, String data, long timeStamp);

	void writeStreamData(long dataId, String data, long timeStamp);

	void writeBatchData(List <VizData> data);

	void writeStreamData(List <VizData> data);

	void writeStreamMeta(VizOpMeta meta);

	void writeBatchMeta(VizOpMeta meta);
}

