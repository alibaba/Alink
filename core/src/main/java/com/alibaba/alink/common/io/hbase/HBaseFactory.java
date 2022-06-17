package com.alibaba.alink.common.io.hbase;

import org.apache.flink.ml.api.misc.param.Params;

import java.io.IOException;

public interface HBaseFactory {
	HBase create(Params params) throws IOException;
}
