package com.alibaba.alink.operator.common.stream.model;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.mapper.ModelMapper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PredictProcess extends RichCoFlatMapFunction <Row, Row, Row> {

	private final DataBridge dataBridge;
	private ModelMapper mapper;
	private final Map <Timestamp, List <Row>> buffers = new HashMap <>();
	private final int timestampColIndex;
	private final int countColIndex;

	public PredictProcess(
		TableSchema modelSchema, TableSchema dataSchema, Params params,
		TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
		DataBridge dataBridge, int timestampColIndex, int countColIndex) {

		this.dataBridge = dataBridge;
		this.mapper = mapperBuilder.apply(modelSchema, dataSchema, params);
		this.timestampColIndex = timestampColIndex;
		this.countColIndex = countColIndex;
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		if (dataBridge != null) {
			// read init model
			List <Row> modelRows = DirectReader.directRead(dataBridge);
			this.mapper.loadModel(modelRows);
			this.mapper.open();
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		this.mapper.close();
	}

	@Override
	public void flatMap1(Row row, Collector <Row> collector) throws Exception {
		collector.collect(this.mapper.map(row));
	}

	@Override
	public void flatMap2(Row inRow, Collector <Row> collector) throws Exception {
		Timestamp timestamp = (Timestamp) inRow.getField(timestampColIndex);
		long count = (long) inRow.getField(countColIndex);

		Row row = ModelStreamUtils.genRowWithoutIdentifier(inRow, timestampColIndex, countColIndex);

		if (buffers.containsKey(timestamp) && buffers.get(timestamp).size() == (int) count - 1) {
			if (buffers.containsKey(timestamp)) {
				buffers.get(timestamp).add(row);
			} else {
				List <Row> buffer = new ArrayList <>(0);
				buffer.add(row);
				buffers.put(timestamp, buffer);
			}

			ModelMapper modelMapper = this.mapper.createNew(buffers.get(timestamp));
			modelMapper.open();

			this.mapper = modelMapper;

			buffers.get(timestamp).clear();
		} else {
			if (buffers.containsKey(timestamp)) {
				buffers.get(timestamp).add(row);
			} else {
				List <Row> buffer = new ArrayList <>(0);
				buffer.add(row);
				buffers.put(timestamp, buffer);
			}
		}
	}
}
