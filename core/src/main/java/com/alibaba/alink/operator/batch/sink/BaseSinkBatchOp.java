package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;

/**
 * The base class for all data sinks.
 * @param <T>
 */
public abstract class BaseSinkBatchOp<T extends BaseSinkBatchOp <T>> extends BatchOperator<T> {

	static final IOType IO_TYPE = IOType.SinkBatch;

	protected BaseSinkBatchOp(String nameSrcSnk, Params params) {
		super(params);
		this.getParams().set(HasIoType.IO_TYPE, AnnotationUtils.annotatedIoType(this.getClass()))
			.set(HasIoName.IO_NAME, nameSrcSnk);

	}

	@Override
	public T linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		return sinkFrom(in.link(new VectorSerializeBatchOp().setMLEnvironmentId(getMLEnvironmentId())));
	}

	protected abstract T sinkFrom(BatchOperator in);

	public static BaseSinkBatchOp of(Params params) throws Exception {
		if (params.contains(HasIoType.IO_TYPE)
			&& params.get(HasIoType.IO_TYPE).equals(IO_TYPE)
			&& params.contains(HasIoName.IO_NAME)) {
			if (BaseDB.isDB(params)) {
				return new DBSinkBatchOp(BaseDB.of(params), params);
			} else if (params.contains(HasIoName.IO_NAME)) {
				String name = params.get(HasIoName.IO_NAME);
				return (BaseSinkBatchOp) AnnotationUtils.createOp(name, IO_TYPE, params);
			}
		}
		throw new RuntimeException("Parameter Error.");

	}
}
