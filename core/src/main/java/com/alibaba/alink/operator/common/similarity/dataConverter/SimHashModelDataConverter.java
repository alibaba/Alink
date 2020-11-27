package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.distance.SimHashHammingDistance;
import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.modeldata.SimHashModelData;
import com.alibaba.alink.operator.common.similarity.similarity.SimHashHammingSimilarity;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;
import scala.util.hashing.MurmurHash3;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimHashModelDataConverter extends NearestNeighborDataConverter <SimHashModelData> {
	private static final long serialVersionUID = -7071296061894969237L;
	private static int ROW_SIZE = 2;
	private static int BUCKETS_INDEX = 0;
	private static int HASHVALUE_IDNEX = 1;
	private static int MAX_ID_NUMBER = 10000;

	private static MurmurHash3 HASH = new MurmurHash3();

	public SimHashModelDataConverter() {
		this.rowSize = ROW_SIZE;
	}

	@Override
	public TableSchema getModelDataSchema() {
		return new TableSchema(new String[] {"BUCKETS", "HASHVALUE"},
			new TypeInformation[] {Types.STRING, Types.STRING});
	}

	@Override
	public SimHashModelData loadModelData(List <Row> list) {
		Map <Integer, List <Object>> indexMap = new HashMap <>();
		Map <Object, BigInteger> data = new HashMap <>();
		for (Row row : list) {
			if (row.getField(BUCKETS_INDEX) != null) {
				Tuple2 <Integer, List <Object>> tuple2 = JsonConverter.fromJson((String) row.getField(BUCKETS_INDEX),
					new TypeReference <Tuple2 <Integer, List <Object>>>() {}.getType());
				List <Object> value = indexMap.get(tuple2.f0);
				if (null != value) {
					tuple2.f1.addAll(value);
				}
				indexMap.put(tuple2.f0, tuple2.f1);
			} else if (row.getField(HASHVALUE_IDNEX) != null) {
				Tuple2 <Object, BigInteger> tuple2 = JsonConverter.fromJson((String) row.getField(HASHVALUE_IDNEX),
					new TypeReference <Tuple2 <Object, BigInteger>>() {}.getType());
				data.put(tuple2.f0, tuple2.f1);
			}
		}
		return new SimHashModelData(indexMap, data, meta.get(StringTextApproxNearestNeighborTrainParams.METRIC),
			meta.get(StringModelDataConverter.TEXT));
	}

	@Override
	public DataSet <Row> buildIndex(BatchOperator in, Params params) {
		DataSet <Row> dataSet = in.getDataSet();
		SimHashHammingSimilarity similarity = new SimHashHammingSimilarity();
		boolean text = params.get(StringModelDataConverter.TEXT);

		DataSet <Tuple2 <Object, BigInteger>> simHash = dataSet.map(
			new MapFunction <Row, Tuple2 <Object, BigInteger>>() {
				private static final long serialVersionUID = 5013768299997778182L;

				@Override
				public Tuple2 <Object, BigInteger> map(Row row) throws Exception {
					String s = (String) row.getField(1);
					Object id = row.getField(0);
					SimHashHammingDistance distance = ((SimHashHammingDistance) similarity.getDistance());
					BigInteger integer = text ? distance.simHash(Sample.split(s)) : distance.simHash(s);
					return Tuple2.of(id, integer);
				}
			});

		DataSet <Row> bucket = simHash.flatMap(
			new FlatMapFunction <Tuple2 <Object, BigInteger>, Tuple2 <Object, Integer>>() {
				private static final long serialVersionUID = -8190719852493750248L;

				@Override
				public void flatMap(Tuple2 <Object, BigInteger> value, Collector <Tuple2 <Object, Integer>> out)
					throws Exception {
					BigInteger integer = value.f1;
					int[] hashes = splitBigInteger(integer);
					for (int hash : hashes) {
						out.collect(Tuple2.of(value.f0, hash));
					}
				}
			}).groupBy(1)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Object, Integer>, Row>() {
				private static final long serialVersionUID = -4943198776190780076L;

				@Override
				public void reduce(Iterable <Tuple2 <Object, Integer>> values, Collector <Row> out) throws Exception {
					List <Object> ids = new ArrayList <>(MAX_ID_NUMBER);
					Integer index = null;
					Row row = new Row(ROW_SIZE);
					for (Tuple2 <Object, Integer> t : values) {
						ids.add(t.f0);
						if (null == index) {
							index = t.f1;
						}
						if (ids.size() > MAX_ID_NUMBER) {
							row.setField(BUCKETS_INDEX, JsonConverter.toJson(Tuple2.of(index, ids)));
							out.collect(row);
							ids.clear();
						}
					}
					row.setField(BUCKETS_INDEX, JsonConverter.toJson(Tuple2.of(index, ids)));
					out.collect(row);
				}
			});

		DataSet <Row> originData = simHash.map(new MapFunction <Tuple2 <Object, BigInteger>, Row>() {
			private static final long serialVersionUID = 8369275450085183950L;

			@Override
			public Row map(Tuple2 <Object, BigInteger> value) throws Exception {
				Row row = new Row(ROW_SIZE);
				row.setField(HASHVALUE_IDNEX, JsonConverter.toJson(value));
				return row;
			}
		});

		return originData.union(bucket)
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 4816437180146626035L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = params;
					}
					new SimHashModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model");
	}

	public static int[] splitBigInteger(BigInteger integer) {
		int[] hashes = new int[4];
		for (int i = 0; i < 4; i++) {
			int shift = 16 * (3 - i);
			BigInteger bit = integer.shiftRight(shift);
			integer = integer.subtract(bit.shiftLeft(shift));
			hashes[i] = HASH.arrayHash(new Integer[] {i, bit.intValue()}, 0);
		}
		return hashes;
	}
}
