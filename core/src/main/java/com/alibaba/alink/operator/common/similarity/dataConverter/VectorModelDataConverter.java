package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceData;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceSparseData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.similarity.modeldata.VectorModelData;
import com.alibaba.alink.params.shared.clustering.HasFastDistanceType;

import java.util.ArrayList;
import java.util.List;

public class VectorModelDataConverter extends NearestNeighborDataConverter <VectorModelData> {
	private static final long serialVersionUID = -3983182343200013328L;
	private static int ROW_SIZE = 2;
	private static int FASTDISTANCE_TYPE_INDEX = 0;
	private static int DATA_INDEX = 1;

	public VectorModelDataConverter() {
		this.rowSize = ROW_SIZE;
	}

	@Override
	public TableSchema getModelDataSchema() {
		return new TableSchema(new String[] {"VECTOR_TYPE", "DATA"},
			new TypeInformation[] {Types.LONG, Types.STRING});
	}

	@Override
	public VectorModelData loadModelData(List <Row> list) {
		List <FastDistanceData> dictData = new ArrayList <>();
		for (Row row : list) {
			if (row.getField(FASTDISTANCE_TYPE_INDEX) != null) {
				long type = (long) row.getField(FASTDISTANCE_TYPE_INDEX);
				if (type == 1L) {
					dictData.add(
						FastDistanceMatrixData.fromString((String) row.getField(DATA_INDEX)));
				} else if (type == 2L) {
					dictData.add(
						FastDistanceVectorData.fromString((String) row.getField(DATA_INDEX)));
				} else if (type == 3L) {
					dictData.add(
						FastDistanceSparseData.fromString((String) row.getField(DATA_INDEX)));
				}
			}
		}
		return new VectorModelData(dictData, meta.get(HasFastDistanceType.DISTANCE_TYPE).getFastDistance());
	}

	@Override
	public DataSet <Row> buildIndex(BatchOperator in, Params params) {
		DataSet <Row> dataSet = in.getDataSet();
		FastDistance fastDistance = params.get(HasFastDistanceType.DISTANCE_TYPE).getFastDistance();

		DataSet <Row> index = dataSet.mapPartition(new RichMapPartitionFunction <Row, Row>() {
			private static final long serialVersionUID = -6035963841026118219L;

			@Override
			public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
				List <FastDistanceData> list = fastDistance.prepareMatrixData(values, 1, 0);
				for (FastDistanceData fastDistanceData : list) {
					Row row = new Row(ROW_SIZE);
					if (fastDistanceData instanceof FastDistanceMatrixData) {
						row.setField(FASTDISTANCE_TYPE_INDEX, 1L);
						FastDistanceMatrixData data = (FastDistanceMatrixData) fastDistanceData;
						row.setField(DATA_INDEX, data.toString());

					} else if (fastDistanceData instanceof FastDistanceVectorData) {
						row.setField(FASTDISTANCE_TYPE_INDEX, 2L);
						FastDistanceVectorData data = (FastDistanceVectorData) fastDistanceData;
						row.setField(DATA_INDEX, data.toString());
					} else if (fastDistanceData instanceof FastDistanceSparseData) {
						row.setField(FASTDISTANCE_TYPE_INDEX, 3L);
						FastDistanceSparseData data = (FastDistanceSparseData) fastDistanceData;
						row.setField(DATA_INDEX, data.toString());
					} else {
						throw new AkUnsupportedOperationException(fastDistanceData.getClass().getName() + " is not supported!");
					}
					out.collect(row);
				}
			}
		});

		return index
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 661383020005730224L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = params;
					}
					new VectorModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model");
	}
}
