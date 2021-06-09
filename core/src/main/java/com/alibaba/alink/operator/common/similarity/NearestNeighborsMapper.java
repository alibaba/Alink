package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.operator.common.similarity.dataConverter.NearestNeighborDataConverter;
import com.alibaba.alink.operator.common.similarity.modeldata.NearestNeighborModelData;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class NearestNeighborsMapper extends SISOModelMapper implements Cloneable {
	private static final long serialVersionUID = 3350330064758380671L;
	private transient NearestNeighborModelData modelData;
	private NearestNeighborDataConverter dataConverter;
	private final Integer topN;
	private final Double radius;

	public NearestNeighborsMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.dataConverter = NearestNeighborDataConverter.DataConverType
			.valueOf(modelSchema.getFieldNames()[modelSchema.getFieldNames().length - 1].toUpperCase())
			.getDataConverter();
		this.dataConverter.setIdType(modelSchema.getFieldTypes()[modelSchema.getFieldNames().length - 1]);
		this.topN = this.params.get(NearestNeighborPredictParams.TOP_N);
		this.radius = this.params.get(NearestNeighborPredictParams.RADIUS);
		Preconditions.checkArgument(!(topN == null && radius == null), "Must give topN or radius!");
	}

	@Override
	public TypeInformation initPredResultColType() {
		return Types.STRING;
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = dataConverter.load(modelRows);
	}

	@Override
	public Object predictResult(Object input) {
		if (null == input) {
			return null;
		}
		return modelData.findNeighbor(input, topN, radius);
	}

	public TypeInformation getIdType() {
		return this.dataConverter.getIdType();
	}

	public Params getMeta() {
		return this.dataConverter.getMeta();
	}

	public static Tuple2 <List <Object>, List <Object>> extractKObject(String s) {
		Map <String, List <Object>> map = KObjectUtil.deserializeKObject(
			s, new String[] {"ID", "METRIC"}, new Type[] {Object.class, Double.class}
		);
		return Tuple2.of(map.get("ID"), map.get("METRIC"));
	}

	public static Tuple2 <List <Object>, List <Object>> extractKObject(String s, Type idType) {
		Map <String, List <Object>> map = KObjectUtil.deserializeKObject(
			s, new String[] {"ID", "METRIC"}, new Type[] {idType, Double.class}
		);
		return Tuple2.of(map.get("ID"), map.get("METRIC"));
	}
}
