package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;

/**
 * Base class for all sinks.
 * @param <T>
 */
public abstract class BaseSinkStreamOp<T extends BaseSinkStreamOp <T>> extends StreamOperator<T> {

	static final IOType IO_TYPE = IOType.SinkStream;

	protected BaseSinkStreamOp(String nameSrcSnk, Params params) {
		super(params);
		this.getParams().set(HasIoType.IO_TYPE, IO_TYPE)
			.set(HasIoName.IO_NAME, nameSrcSnk);
	}

	@Override
	public T linkFrom(StreamOperator<?>... inputs) {
		StreamOperator<?> in = checkAndGetFirst(inputs);
		return sinkFrom(in.link(new VectorSerializeStreamOp().setMLEnvironmentId(getMLEnvironmentId())));
	}

	protected abstract T sinkFrom(StreamOperator in);

	public static BaseSinkStreamOp of(Params params) throws Exception {
		if (params.contains(HasIoType.IO_TYPE)
			&& params.get(HasIoType.IO_TYPE).equals(IO_TYPE)
			&& params.contains(HasIoName.IO_NAME)) {
			if (BaseDB.isDB(params)) {
				return new DBSinkStreamOp(BaseDB.of(params), params);
			} else if (params.contains(HasIoName.IO_NAME)) {
				String name = params.get(HasIoName.IO_NAME);
				return (BaseSinkStreamOp) AnnotationUtils.createOp(name, IO_TYPE, params);
			}
		}
		throw new RuntimeException("Parameter Error.");

	}

	/**
	 * Converts the given Table into a DataStream<Row> of add and retract messages.
	 *
	 * @param table the Table to convert.
	 * @return the converted DataStream.
	 */
	public  DataStream <Row> toRetractStream(Table table) {
		return MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().toRetractStream(table, Row.class)
			.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
				@Override
				public void flatMap(Tuple2 <Boolean, Row> tuple2, Collector <Row> collector) throws Exception {
					if (tuple2.f0) {
						collector.collect(tuple2.f1);
					}
				}
			});
	}

	/**
	 * Judge whether the table only have insert (append) changes.
	 *
	 * @param table the Table to convert.
	 * @return if the Table only have insert (append) changes, return true. If the Table also modifies
	 * by update or delete changes, returns false.
	 */
	public Boolean isAppendStream(Table table) {
		try {
			MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().toAppendStream(table, Row.class);
			return true;
		} catch (org.apache.flink.table.api.TableException ex) {
			return false;
		}
	}
}
