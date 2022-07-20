package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.modeldata.MinHashModelData;
import com.alibaba.alink.operator.common.similarity.similarity.JaccardSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.MinHashSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.SimHashHammingSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.Similarity;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;
import com.alibaba.alink.params.similarity.StringTextApproxParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MinHashModelDataConverter extends NearestNeighborDataConverter <MinHashModelData> {
	private static final long serialVersionUID = -1960235638970152172L;
	private static int ROW_SIZE = 2;
	private static int BUCKETS_INDEX = 0;
	private static int HASHVALUE_IDNEX = 1;
	private static int MAX_ID_NUMBER = 10000;

	public MinHashModelDataConverter() {
		this.rowSize = ROW_SIZE;
	}

	@Override
	public TableSchema getModelDataSchema() {
		return new TableSchema(new String[] {"BUCKETS", "HASHVALUE"},
			new TypeInformation[] {Types.STRING, Types.STRING});
	}

	@Override
	public MinHashModelData loadModelData(List <Row> list) {
		Map <Integer, List <Object>> indexMap = new HashMap <>();
		Map <Object, int[]> data = new HashMap <>();
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
				Tuple2 <Object, int[]> tuple2 = JsonConverter.fromJson((String) row.getField(HASHVALUE_IDNEX),
					new TypeReference <Tuple2 <Object, int[]>>() {}.getType());
				data.put(tuple2.f0, tuple2.f1);
			}
		}
		MinHashSimilarity similarity = (MinHashSimilarity) initSimilarity(meta);
		return new MinHashModelData(indexMap, data, similarity, meta.get(StringModelDataConverter.TEXT));
	}

	@Override
	public DataSet <Row> buildIndex(BatchOperator in, Params params) {
		DataSet <Row> dataSet = in.getDataSet();
		MinHashSimilarity similarity = (MinHashSimilarity) initSimilarity(params);
		boolean text = params.get(StringModelDataConverter.TEXT);

		DataSet <Tuple3 <Object, int[], int[]>> hashValue = dataSet.map(
			new MapFunction <Row, Tuple3 <Object, int[], int[]>>() {
				private static final long serialVersionUID = -7873937415892360963L;

				@Override
				public Tuple3 <Object, int[], int[]> map(Row row) throws Exception {
					String s = (String) row.getField(1);
					Object id = row.getField(0);
					int[] sorted = similarity.getSorted(text ? Sample.split(s) : s);
					int[] hashValue = similarity.getMinHash(sorted);
					return Tuple3.of(id, hashValue, sorted);
				}
			});

		DataSet <Row> bucket = hashValue.flatMap(
			new FlatMapFunction <Tuple3 <Object, int[], int[]>, Tuple2 <Object, Integer>>() {
				private static final long serialVersionUID = -6806968610227512347L;

				@Override
				public void flatMap(Tuple3 <Object, int[], int[]> value, Collector <Tuple2 <Object, Integer>> out)
					throws Exception {
					int[] bucket = similarity.toBucket(value.f1);
					for (int aBucket : bucket) {
						out.collect(Tuple2.of(value.f0, aBucket));
					}
				}
			}).groupBy(1)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Object, Integer>, Row>() {
				private static final long serialVersionUID = -5375727063522767320L;

				@Override
				public void reduce(Iterable <Tuple2 <Object, Integer>> values, Collector <Row> out) throws Exception {
					List <Object> ids = new ArrayList <>();
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

		DataSet <Row> originData = hashValue.map(new MapFunction <Tuple3 <Object, int[], int[]>, Row>() {
			private static final long serialVersionUID = 5837972589645286085L;

			@Override
			public Row map(Tuple3 <Object, int[], int[]> value) throws Exception {
				Row row = new Row(ROW_SIZE);
				row.setField(HASHVALUE_IDNEX, JsonConverter.toJson(Tuple2.of(value.f0,
					similarity instanceof JaccardSimilarity ? value.f2 : value.f1)));
				return row;
			}
		});

		return originData.union(bucket)
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = -5812648057331004232L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = params;
					}
					new MinHashModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model");

	}

	private Similarity initSimilarity(Params params) {
		switch (params.get(StringTextApproxNearestNeighborTrainParams.METRIC)) {
			case MINHASH_JACCARD_SIM: {
				return new MinHashSimilarity(params.get(StringTextApproxParams.SEED),
					params.get(StringTextApproxParams.NUM_HASH_TABLES),
					params.get(StringTextApproxParams.NUM_BUCKET));
			}
			case JACCARD_SIM: {
				return new JaccardSimilarity(params.get(StringTextApproxParams.SEED),
					params.get(StringTextApproxParams.NUM_HASH_TABLES),
					params.get(StringTextApproxParams.NUM_BUCKET));
			}
			case SIMHASH_HAMMING_SIM: {
				return new SimHashHammingSimilarity();
			}
			default: {
				throw new AkUnsupportedOperationException(params.get(StringTextApproxNearestNeighborTrainParams.METRIC).toString() + " is not supported");
			}
		}
	}
}
