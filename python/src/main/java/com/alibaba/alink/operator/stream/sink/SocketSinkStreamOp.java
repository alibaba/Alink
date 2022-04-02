package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.SocketSinkParams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import scala.Serializable;

import java.nio.charset.StandardCharsets;

@IoOpAnnotation(name = "socket_sink", ioType = IOType.SinkStream)
@NameCn("")
public class SocketSinkStreamOp extends BaseSinkStreamOp <SocketSinkStreamOp>
	implements SocketSinkParams <SocketSinkStreamOp> {

	public SocketSinkStreamOp() {
		this(new Params());
	}

	public SocketSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(SocketSinkStreamOp.class), params);
	}

	@Override
	public SocketSinkStreamOp sinkFrom(StreamOperator <?> in) {
		DataStreamSink <?> returnStream = in.getDataStream().addSink(
			new SocketClientSink <>(getHost(), getPort(), new PyRowSerializationSchema(), 0, true));
		returnStream.setParallelism(1); // It would not work if multiple instances would connect to the same port
		return this;
	}

	public static class LegacyRow implements Serializable {
		public Object[] fields;
	}

	public static class PyRowSerializationSchema implements SerializationSchema <Row> {

		public Gson gson;
		public LegacyRow legacyRow = new LegacyRow();

		@Override
		public byte[] serialize(Row element) {
			if (null == gson) {
				gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
			}
			legacyRow.fields = new Object[element.getArity()];
			for (int i = 0; i < element.getArity(); i += 1) {
				legacyRow.fields[i] = element.getField(i);
			}
			String str = gson.toJson(legacyRow) + "\r\n";
			return str.getBytes(StandardCharsets.UTF_8);
		}
	}
}
