package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.dummy.DummyOutputFormat;

@IoOpAnnotation(name = "dummy", ioType = IOType.SinkBatch)
@NameCn("DummySink")
@NameEn("DummySink")
public final class DummySinkBatchOp extends BaseSinkBatchOp <DummySinkBatchOp> {

	private static final long serialVersionUID = -676597300950259541L;

	public DummySinkBatchOp() {
		this(new Params());
	}

	public DummySinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(DummySinkBatchOp.class), params);
	}

	@Override
	public DummySinkBatchOp sinkFrom(BatchOperator<?> in) {
		in.getDataSet().output(new DummyOutputFormat <>());
		return this;
	}

}
