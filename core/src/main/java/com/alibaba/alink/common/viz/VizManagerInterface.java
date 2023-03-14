package com.alibaba.alink.common.viz;

import org.apache.flink.ml.api.misc.param.Params;

import java.io.Serializable;

public interface VizManagerInterface extends Serializable {

	VizDataWriterInterface getVizDataWriter(Params params);

	String getVizName();

}
