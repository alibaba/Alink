package com.alibaba.alink.operator.stream.source;

import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.IOType;

import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * Base class for all stream source.
 * @param <T>
 */
public abstract class BaseSourceStreamOp<T extends BaseSourceStreamOp <T>> extends StreamOperator <T> {

	static final IOType IO_TYPE = IOType.SourceStream;

	protected BaseSourceStreamOp(String nameSrcSnk, Params params) {
		super(params);
		this.getParams().set(HasIoType.IO_TYPE, IO_TYPE)
			.set(HasIoName.IO_NAME, nameSrcSnk);
	}

	public static BaseSourceStreamOp of(Params params) throws Exception {
		if (params.contains(HasIoType.IO_TYPE)
			&& params.get(HasIoType.IO_TYPE).equals(IO_TYPE)
			&& params.contains(HasIoName.IO_NAME)) {
			if (BaseDB.isDB(params)) {
				return new DBSourceStreamOp(BaseDB.of(params), params);
			} else if (params.contains(HasIoName.IO_NAME)) {
				String name = params.get(HasIoName.IO_NAME);
				return (BaseSourceStreamOp) AnnotationUtils.createOp(name, IO_TYPE, params);
			}
		}
		throw new RuntimeException("Parameter Error.");
	}

	@Override
	public T linkFrom(StreamOperator<?>... inputs) {
		throw new UnsupportedOperationException("Source operator does not support linkFrom()");
	}

	@Override
	public Table getOutputTable() {
		if (super.getOutputTable() == null) {
			super.setOutputTable(initializeDataSource());
		}
		return super.getOutputTable();
	}

	protected abstract Table initializeDataSource();
}
