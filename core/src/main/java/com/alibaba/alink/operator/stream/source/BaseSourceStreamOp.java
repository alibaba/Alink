package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;

/**
 * Base class for all stream source.
 *
 * @param <T>
 */
@InputPorts()
@OutputPorts(values = {@PortSpec(PortType.ANY)})
@NameCn("")
public abstract class BaseSourceStreamOp<T extends BaseSourceStreamOp <T>> extends StreamOperator <T> {

	static final IOType IO_TYPE = IOType.SourceStream;
	private static final long serialVersionUID = 9039423063731929088L;

	protected BaseSourceStreamOp(String nameSrcSnk, Params params) {
		super(params);
		getParams().set(HasIoType.IO_TYPE, IO_TYPE)
			.set(HasIoName.IO_NAME, nameSrcSnk);
	}

	@Override
	public final T linkFrom(StreamOperator <?>... inputs) {
		throw new UnsupportedOperationException("Source operator does not support linkFrom()");
	}

	@Override
	public Table getOutputTable() {
		if (isNullOutputTable()) {
			super.setOutputTable(initializeDataSource());
		}
		return super.getOutputTable();
	}

	protected abstract Table initializeDataSource();
}
