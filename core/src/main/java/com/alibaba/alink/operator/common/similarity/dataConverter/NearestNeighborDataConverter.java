package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.similarity.modeldata.NearestNeighborModelData;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.List;

public abstract class NearestNeighborDataConverter<T extends NearestNeighborModelData> implements
	ModelDataConverter <Tuple2 <Params, Iterable <Row>>, T>, Serializable {
	private static final long serialVersionUID = 8675166240772682009L;
	private TypeInformation idType;
	protected int rowSize;

	protected Params meta;

	public static ParamInfo <String> ID_TYPE = ParamInfoFactory
		.createParamInfo("idType", String.class)
		.setDescription("id type")
		.setRequired()
		.build();

	public NearestNeighborDataConverter() {
	}

	public TypeInformation getIdType() {
		return idType;
	}

	public Params getMeta() {
		return meta;
	}

	public void setIdType(TypeInformation idType) {
		this.idType = idType;
	}

	public enum DataConverType {
		KDTREEMODELDATACONVERTER(new KDTreeModelDataConverter()),

		LSHMODELDATACONVERTER(new LSHModelDataConverter()),

		MINHASHMODELDATACONVERTER(new MinHashModelDataConverter()),

		SIMHASHMODELDATACONVERTER(new SimHashModelDataConverter()),

		STRINGMODELDATACONVERTER(new StringModelDataConverter()),

		VECTORMODELDATACONVERTER(new VectorModelDataConverter()),

		LOCALLSHMODELDATACONVERTER(new LocalLSHModelDataConverter());

		public NearestNeighborDataConverter getDataConverter() {
			return dataConverter;
		}

		private NearestNeighborDataConverter dataConverter;

		DataConverType(NearestNeighborDataConverter dataConverter) {
			this.dataConverter = dataConverter;
		}

		public static DataConverType fromDataConverter(NearestNeighborDataConverter dataConverter) {
			return valueOf(dataConverter.getClass().getSimpleName().toUpperCase());
		}
	}

	@Override
	public TableSchema getModelSchema() {
		TableSchema tableSchema = getModelDataSchema();
		AkPreconditions.checkNotNull(idType, "ID type not set!");
		return new TableSchema(
			ArrayUtils.addAll(tableSchema.getFieldNames(), "META", DataConverType.fromDataConverter(this).name()),
			ArrayUtils.addAll(tableSchema.getFieldTypes(), Types.STRING, idType));
	}

	@Override
	public void save(Tuple2 <Params, Iterable <Row>> modelData, Collector <Row> collector) {
		if (modelData.f0 != null) {
			Row row = new Row(this.rowSize + 2);
			row.setField(this.rowSize, modelData.f0.toJson());
			collector.collect(row);
		}

		modelData.f1.forEach(r -> collector.collect(RowUtil.merge(r, new Row(2))));
	}

	@Override
	public T load(List <Row> list) {
		for (Row row : list) {
			if (row.getField(row.getArity() - 2) != null) {
				this.meta = Params.fromJson((String) row.getField(row.getArity() - 2));
			}
		}
		T data = this.loadModelData(list);
		data.setIdType(FlinkTypeConverter.getFlinkType(meta.get(NearestNeighborDataConverter.ID_TYPE)));
		return data;
	}

	public abstract DataSet <Row> buildIndex(BatchOperator in, Params params);

	public List<Row> buildIndex(MTable mt, Params params) {
		throw new AkUnsupportedOperationException("To be implemented.");
	}

	protected abstract TableSchema getModelDataSchema();

	protected abstract T loadModelData(List <Row> list);
}
