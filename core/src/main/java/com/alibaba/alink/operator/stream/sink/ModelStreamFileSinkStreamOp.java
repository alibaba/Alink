package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.dummy.DummyOutputFormat;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils.FileModelStreamSink;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.ModelStreamFileSinkParams;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@IoOpAnnotation(name = "modelstream_file", ioType = IOType.SinkStream)
public final class ModelStreamFileSinkStreamOp extends BaseSinkStreamOp <ModelStreamFileSinkStreamOp>
	implements ModelStreamFileSinkParams <ModelStreamFileSinkStreamOp> {

	private static final long serialVersionUID = 4650091128460845189L;

	public ModelStreamFileSinkStreamOp() {
		super(AnnotationUtils.annotatedName(ModelStreamFileSinkStreamOp.class), new Params());
	}

	public ModelStreamFileSinkStreamOp(Params parameter) {
		super(AnnotationUtils.annotatedName(ModelStreamFileSinkStreamOp.class), parameter);
	}

	@Override
	public ModelStreamFileSinkStreamOp sinkFrom(StreamOperator <?> in) {

		TableSchema schema = in.getSchema();

		final int timestampColIndex = ModelStreamUtils.findTimestampColIndexWithAssertAndHint(schema);
		final int countColIndex = ModelStreamUtils.findCountColIndexWithAssertAndHint(schema);

		final TableSchema dataSchema = new TableSchema(
			ArrayUtils.removeAll(schema.getFieldNames(), timestampColIndex, countColIndex),
			ArrayUtils.removeAll(schema.getFieldTypes(), timestampColIndex, countColIndex)
		);

		final String dataSchemaStr = CsvUtil.schema2SchemaStr(dataSchema);

		final FilePath path = getFilePath();
		final int numKeepModel = getNumKeepModel();

		final ModelStreamUtils.FileModelStreamSink fileModelStreamSink
			= new FileModelStreamSink(path, dataSchemaStr);

		try {
			fileModelStreamSink.initializeGlobal();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

		DataStream <Row> inputStream = in
			.getDataStream();

		DataStream <Tuple4 <Timestamp, Integer, Long, Long>> count = inputStream
			.map(new RichMapFunction <Row, Tuple4 <Timestamp, Integer, Long, Long>>() {
				@Override
				public Tuple4 <Timestamp, Integer, Long, Long> map(Row value) {
					return Tuple4.of(
						(Timestamp) value.getField(timestampColIndex),
						getRuntimeContext().getIndexOfThisSubtask(),
						(Long) value.getField(countColIndex),
						1L
					);
				}
			})
			.keyBy(0, 1, 2)
			.reduce(new ReduceFunction <Tuple4 <Timestamp, Integer, Long, Long>>() {
				@Override
				public Tuple4 <Timestamp, Integer, Long, Long> reduce(
					Tuple4 <Timestamp, Integer, Long, Long> value1, Tuple4 <Timestamp, Integer, Long, Long> value2) {

					return Tuple4.of(
						value1.f0,
						value1.f1,
						value1.f2,
						value1.f3 + value2.f3
					);
				}
			})
			.keyBy(0, 2)
			.flatMap(
				new RichFlatMapFunction <Tuple4 <Timestamp, Integer, Long, Long>, Tuple4 <Timestamp, Integer, Long,
					Long>>() {

					private transient MapState <Integer, Tuple4 <Timestamp, Integer, Long, Long>> latest;

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);

						latest = getRuntimeContext().getMapState(
							new MapStateDescriptor <>(
								"latest", Types.INT,
								new TupleTypeInfo <>(Types.SQL_TIMESTAMP, Types.INT, Types.LONG, Types.LONG)
							)
						);
					}

					@Override
					public void flatMap(Tuple4 <Timestamp, Integer, Long, Long> value,
										Collector <Tuple4 <Timestamp, Integer, Long, Long>> out) throws Exception {

						latest.put(value.f1, value);

						long sum = 0;
						long total = -1;

						for (Map.Entry <Integer, Tuple4 <Timestamp, Integer, Long, Long>> entry : latest.entries()) {
							total = entry.getValue().f2;
							sum += entry.getValue().f3;
						}

						if (total == sum) {
							// done

							for (Map.Entry <Integer, Tuple4 <Timestamp, Integer, Long, Long>> entry :
								latest.entries()) {
								out.collect(entry.getValue());
							}
						}
					}
				});

		inputStream
			.map(new RichMapFunction <Row, Tuple3 <Timestamp, Integer, Row>>() {
				@Override
				public Tuple3 <Timestamp, Integer, Row> map(Row value) {
					return Tuple3.of(
						(Timestamp) value.getField(timestampColIndex),
						getRuntimeContext().getIndexOfThisSubtask(),
						ModelStreamUtils.genRowWithoutIdentifier(value, timestampColIndex, countColIndex)
					);
				}
			})
			.keyBy(0, 1)
			.connect(count.keyBy(0, 1))
			.flatMap(
				new RichCoFlatMapFunction <Tuple3 <Timestamp, Integer, Row>, Tuple4 <Timestamp, Integer, Long, Long>,
					Tuple1 <Timestamp>>() {

					private final Map <Tuple2 <Timestamp, Integer>, Tuple3 <FileModelStreamSink, Long, Long>>
						writerContainer = new HashMap <>();

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);
					}

					@Override
					public void flatMap1(Tuple3 <Timestamp, Integer, Row> value, Collector <Tuple1 <Timestamp>> out) {
						writerContainer.compute(
							Tuple2.of(value.f0, value.f1),
							(key, oldValue) -> {
								if (oldValue == null) {
									FileModelStreamSink localFileModelStreamSink
										= new FileModelStreamSink(path, dataSchemaStr);

									try {
										localFileModelStreamSink.open(value.f0, value.f1);
									} catch (IOException e) {
										throw new RuntimeException(e);
									}

									localFileModelStreamSink.collect(value.f2);

									return Tuple3.of(localFileModelStreamSink, 1L, null);
								} else if (oldValue.f0 == null) {
									FileModelStreamSink localFileModelStreamSink
										= new FileModelStreamSink(path, dataSchemaStr);

									try {
										localFileModelStreamSink.open(value.f0, value.f1);
									} catch (IOException e) {
										throw new RuntimeException(e);
									}

									localFileModelStreamSink.collect(value.f2);

									if (oldValue.f2 != null && oldValue.f2.equals(1L)) {
										localFileModelStreamSink.close();
										out.collect(Tuple1.of(key.f0));
										return null;
									} else {
										return Tuple3.of(localFileModelStreamSink, 1L, null);
									}
								} else {
									oldValue.f0.collect(value.f2);
									++oldValue.f1;

									if (oldValue.f2 != null && oldValue.f2.equals(oldValue.f1)) {
										oldValue.f0.close();
										out.collect(Tuple1.of(key.f0));
										return null;
									} else {
										return Tuple3.of(oldValue.f0, oldValue.f1, oldValue.f2);
									}
								}
							}
						);
					}

					@Override
					public void flatMap2(Tuple4 <Timestamp, Integer, Long, Long> value,
										 Collector <Tuple1 <Timestamp>> out) {
						writerContainer.compute(Tuple2.of(value.f0, value.f1),
							(key, oldValue) -> {
								if (oldValue == null) {
									return Tuple3.of(null, null, value.f3);
								} else {
									if (value.f3.equals(oldValue.f1)) {
										oldValue.f0.close();
										out.collect(Tuple1.of(key.f0));
										return null;
									} else {
										return Tuple3.of(oldValue.f0, oldValue.f1, value.f3);
									}
								}
							}
						);
					}
				})
			.keyBy(0)
			.connect(
				count.keyBy(0)
					.flatMap(
						new RichFlatMapFunction <Tuple4 <Timestamp, Integer, Long, Long>, Tuple4 <Timestamp, Integer,
							Integer, Long>>() {

							private transient ListState <Tuple2 <Integer, Long>> filesCounter;

							@Override
							public void open(Configuration parameters) throws Exception {
								super.open(parameters);

								filesCounter = getRuntimeContext().getListState(
									new ListStateDescriptor <>(
										"filesCounter",
										new TupleTypeInfo <>(Types.INT, Types.LONG)
									)
								);
							}

							@Override
							public void flatMap(Tuple4 <Timestamp, Integer, Long, Long> value,
												Collector <Tuple4 <Timestamp, Integer, Integer, Long>> out)
								throws Exception {

								Long sum = value.f3;
								Integer fileCount = 1;

								List <Tuple2 <Integer, Long>> local = new ArrayList <>();
								local.add(Tuple2.of(value.f1, value.f3));

								for (Tuple2 <Integer, Long> count : filesCounter.get()) {
									sum += count.f1;
									fileCount++;
									local.add(count);
								}

								if (value.f2.equals(sum)) {
									for (Tuple2 <Integer, Long> count : local) {
										out.collect(Tuple4.of(value.f0, count.f0, fileCount, value.f2));
									}
								}

								filesCounter.add(Tuple2.of(value.f1, value.f3));
							}
						})
					.keyBy(0)
			)
			.flatMap(
				new RichCoFlatMapFunction <Tuple1 <Timestamp>, Tuple4 <Timestamp, Integer, Integer, Long>, byte[]>() {
					private transient ValueState <Integer> filesCounter;
					private transient ListState <Tuple3 <Integer, Integer, Long>> total;

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);

						filesCounter = getRuntimeContext().getState(
							new ValueStateDescriptor <>("filesCounter", Types.INT));

						total = getRuntimeContext().getListState(
							new ListStateDescriptor <>("total", new TupleTypeInfo <>(Types.INT, Types.LONG))
						);
					}

					@Override
					public void flatMap1(Tuple1 <Timestamp> value, Collector <byte[]> out) throws Exception {
						Integer count = filesCounter.value();

						if (count == null) {
							count = 1;
						} else {
							++count;
						}

						List <Tuple3 <Integer, Integer, Long>> local = new ArrayList <>();

						for (Tuple3 <Integer, Integer, Long> t : total.get()) {
							local.add(t);
						}

						if (!local.isEmpty()
							&& local.get(0).f1.equals(local.size())
							&& local.get(0).f1.equals(count)) {

							List <Integer> filesId = new ArrayList <>();

							for (Tuple3 <Integer, Integer, Long> t : local) {
								filesId.add(t.f0);
							}

							new FileModelStreamSink(path, dataSchemaStr)
								.finalizeGlobal(value.f0, local.get(0).f2, filesId, numKeepModel);
						}

						filesCounter.update(count);
					}

					@Override
					public void flatMap2(Tuple4 <Timestamp, Integer, Integer, Long> value, Collector <byte[]> out)
						throws Exception {
						List <Tuple3 <Integer, Integer, Long>> local = new ArrayList <>();
						local.add(Tuple3.of(value.f1, value.f2, value.f3));

						for (Tuple3 <Integer, Integer, Long> t : total.get()) {
							local.add(t);
						}

						if (local.get(0).f1.equals(local.size())
							&& local.get(0).f1.equals(filesCounter.value())) {

							List <Integer> filesId = new ArrayList <>();

							for (Tuple3 <Integer, Integer, Long> t : local) {
								filesId.add(t.f0);
							}

							new FileModelStreamSink(path, dataSchemaStr)
								.finalizeGlobal(value.f0, local.get(0).f2, filesId, numKeepModel);
						}

						total.add(Tuple3.of(value.f1, value.f2, value.f3));
					}
				})
			.writeUsingOutputFormat(new DummyOutputFormat <>());

		return this;
	}
}
