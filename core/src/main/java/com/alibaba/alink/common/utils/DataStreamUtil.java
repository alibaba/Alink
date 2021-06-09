package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils for handling datastream.
 */
@SuppressWarnings("unchecked")
public class DataStreamUtil {

	/**
	 * Stack a datastream of rows
	 */
	public static DataStream <List <Row>> stack(DataStream <Row> input, final int size) {
		return input
			.flatMap(new RichFlatMapFunction <Row, List <Row>>() {
				private static final long serialVersionUID = -2909825492775487009L;
				transient Collector <List <Row>> collector;
				transient List <Row> buffer;

				@Override
				public void open(Configuration parameters) throws Exception {
					this.buffer = new ArrayList <>();
				}

				@Override
				public void close() throws Exception {
					if (this.buffer.size() > 0) {
						this.collector.collect(this.buffer);
						this.buffer.clear();
					}
				}

				@Override
				public void flatMap(Row value, Collector <List <Row>> out) throws Exception {
					this.collector = out;
					this.buffer.add(value);
					if (this.buffer.size() >= size) {
						this.collector.collect(this.buffer);
						this.buffer.clear();
					}
				}
			});
	}

}
