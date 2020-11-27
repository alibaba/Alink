package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.similarity.LocalitySensitiveHashApproxFunctions;
import com.alibaba.alink.operator.common.similarity.lsh.BaseLSH;
import com.alibaba.alink.operator.common.similarity.lsh.BucketRandomProjectionLSH;
import com.alibaba.alink.operator.common.similarity.lsh.MinHashLSH;
import com.alibaba.alink.operator.common.similarity.modeldata.LSHModelData;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LSHModelDataConverter extends NearestNeighborDataConverter <LSHModelData> {
	private static final long serialVersionUID = -6846015825612538416L;
	private static int ROW_SIZE = 2;
	private static int BUCKETS_INDEX = 0;
	private static int DATA_IDNEX = 1;

	public LSHModelDataConverter() {
		this.rowSize = ROW_SIZE;
	}

	@Override
	public TableSchema getModelDataSchema() {
		return new TableSchema(new String[] {"BUCKETS", "DATA"},
			new TypeInformation[] {Types.STRING, Types.STRING});
	}

	@Override
	public LSHModelData loadModelData(List <Row> list) {
		Map <Integer, List <Object>> indexMap = new HashMap <>();
		Map <Object, Vector> data = new HashMap <>();
		for (Row row : list) {
			if (row.getField(BUCKETS_INDEX) != null) {
				Tuple2 <Integer, List <Object>> tuple2 = JsonConverter.fromJson((String) row.getField(BUCKETS_INDEX),
					new TypeReference <Tuple2 <Integer, List <Object>>>() {}.getType());
				indexMap.put(tuple2.f0, tuple2.f1);
			} else if (row.getField(DATA_IDNEX) != null) {
				Tuple2 <Object, String> tuple3 = JsonConverter.fromJson((String) row.getField(DATA_IDNEX),
					new TypeReference <Tuple2 <Object, String>>() {}.getType());
				data.put(tuple3.f0, VectorUtil.getVector(tuple3.f1));
			}
		}
		BaseLSH lsh;
		if (meta.get(VectorApproxNearestNeighborTrainParams.METRIC).equals(
			VectorApproxNearestNeighborTrainParams.Metric.JACCARD)) {
			lsh = new MinHashLSH(meta.get(MinHashLSH.RAND_COEFFICIENTS_A), meta.get(MinHashLSH.RAND_COEFFICIENTS_B));
		} else {
			lsh = new BucketRandomProjectionLSH(meta.get(BucketRandomProjectionLSH.RAND_VECTORS),
				meta.get(BucketRandomProjectionLSH.RAND_NUMBER), meta.get(BucketRandomProjectionLSH.PROJECTION_WIDTH));
		}
		return new LSHModelData(indexMap, data, lsh);
	}

	@Override
	public DataSet <Row> buildIndex(BatchOperator in, Params params) {
		DataSet <BaseLSH> lsh = LocalitySensitiveHashApproxFunctions.buildLSH(
			in, params, params.get(VectorApproxNearestNeighborTrainParams.SELECTED_COL));

		DataSet <Tuple3 <Object, Vector, int[]>> hashValue = in.getDataSet().map(
			new RichMapFunction <Row, Tuple3 <Object, Vector, int[]>>() {
				private static final long serialVersionUID = 9119201008956936115L;

				@Override
				public Tuple3 <Object, Vector, int[]> map(Row row) throws Exception {
					BaseLSH lsh = (BaseLSH) getRuntimeContext().getBroadcastVariable("lsh").get(0);
					Vector vector = VectorUtil.getVector(row.getField(1));
					Object id = row.getField(0);
					int[] hashValue = lsh.hashFunction(vector);
					return Tuple3.of(id, vector, hashValue);
				}
			}).withBroadcastSet(lsh, "lsh");

		DataSet <Row> bucket = hashValue.flatMap(
			new FlatMapFunction <Tuple3 <Object, Vector, int[]>, Tuple2 <Object, Integer>>() {
				private static final long serialVersionUID = 7401684044391240070L;

				@Override
				public void flatMap(Tuple3 <Object, Vector, int[]> value, Collector <Tuple2 <Object, Integer>> out)
					throws Exception {
					for (int aBucket : value.f2) {
						out.collect(Tuple2.of(value.f0, aBucket));
					}
				}
			}).groupBy(1)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Object, Integer>, Row>() {
				private static final long serialVersionUID = -4976135470912551698L;

				@Override
				public void reduce(Iterable <Tuple2 <Object, Integer>> values, Collector <Row> out) throws Exception {
					List <Object> ids = new ArrayList <>();
					Integer index = null;
					for (Tuple2 <Object, Integer> t : values) {
						ids.add(t.f0);
						if (null == index) {
							index = t.f1;
						}
					}
					Row row = new Row(ROW_SIZE);
					row.setField(BUCKETS_INDEX, JsonConverter.toJson(Tuple2.of(index, ids)));
					out.collect(row);
				}
			});

		DataSet <Row> originData = hashValue.map(new MapFunction <Tuple3 <Object, Vector, int[]>, Row>() {
			private static final long serialVersionUID = 7915820872982890995L;

			@Override
			public Row map(Tuple3 <Object, Vector, int[]> value) throws Exception {
				Row row = new Row(ROW_SIZE);
				row.setField(DATA_IDNEX, JsonConverter.toJson(Tuple2.of(value.f0, value.f1.toString())));
				return row;
			}
		});

		return originData.union(bucket)
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 1398487522497229248L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out) {
					Params meta = null;
					BaseLSH lsh = (BaseLSH) getRuntimeContext().getBroadcastVariable("lsh").get(0);
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = params;
						if (lsh instanceof BucketRandomProjectionLSH) {
							BucketRandomProjectionLSH brpLsh = (BucketRandomProjectionLSH) lsh;
							meta.set(BucketRandomProjectionLSH.RAND_VECTORS, brpLsh.getRandVectors())
								.set(BucketRandomProjectionLSH.RAND_NUMBER, brpLsh.getRandNumber())
								.set(BucketRandomProjectionLSH.PROJECTION_WIDTH, brpLsh.getProjectionWidth());
						} else {
							MinHashLSH minHashLSH = (MinHashLSH) lsh;
							meta.set(MinHashLSH.RAND_COEFFICIENTS_A, minHashLSH.getRandCoefficientsA())
								.set(MinHashLSH.RAND_COEFFICIENTS_B, minHashLSH.getRandCoefficientsB());
						}
					}
					new LSHModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.withBroadcastSet(lsh, "lsh")
			.name("build_model");

	}
}
