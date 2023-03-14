package com.alibaba.alink.common.viz;

import java.util.List;

public class DummyVizDataWriter implements VizDataWriterInterface {
	private static final long serialVersionUID = -3525933894792429368L;

	@Override
	public void writeBatchData(long dataType, String data, long timeStamp) {

	}

	@Override
	public void writeStreamData(long dataType, String data, long timeStamp) {

	}

	@Override
	public void writeBatchData(List <VizData> data) {

	}

	@Override
	public void writeStreamData(List <VizData> data) {

	}

	@Override
	public void writeBatchMeta(VizOpMeta meta) {

	}

	@Override
	public void writeStreamMeta(VizOpMeta meta) {

	}

}
