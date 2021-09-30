package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.MTableSerializeBatchOp;
import com.alibaba.alink.operator.batch.utils.TensorSerializeBatchOp;
import com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;

/**
 * The base class for all data sinks.
 *
 * @param <T>
 */
public abstract class BaseSinkBatchOp<T extends BaseSinkBatchOp <T>> extends BatchOperator <T> {

	static final IOType IO_TYPE = IOType.SinkBatch;
	private static final long serialVersionUID = 8092537809636348225L;

	protected BaseSinkBatchOp(String nameSrcSnk, Params params) {
		super(params);
		this.getParams().set(HasIoType.IO_TYPE, AnnotationUtils.annotatedIoType(this.getClass()))
			.set(HasIoName.IO_NAME, nameSrcSnk);

	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		return sinkFrom(in
			.link(new VectorSerializeBatchOp().setMLEnvironmentId(getMLEnvironmentId()))
			.link(new MTableSerializeBatchOp().setMLEnvironmentId(getMLEnvironmentId()))
			.link(new TensorSerializeBatchOp().setMLEnvironmentId(getMLEnvironmentId()))
		);
	}

	protected abstract T sinkFrom(BatchOperator<?> in);

	@Override
	public final Table getOutputTable() {
		throw new RuntimeException("Sink Operator has no output data.");
	}

	@Override
	public final BatchOperator<?> getSideOutput(int idx) {
		throw new RuntimeException("Sink Operator has no side-output data.");
	}

	@Override
	public final int getSideOutputCount() {
		return 0;
	}

}
