package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanConstant;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanNewSample;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.params.clustering.GroupDbscanParams;
import com.alibaba.alink.params.clustering.HasLatitudeCol;
import com.alibaba.alink.params.clustering.HasLongitudeCol;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * @author guotao.gt
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT),
})
@ReservedColsWithSecondInputSpec
@ParamSelectColumnSpec(name = "featureCols", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ParamSelectColumnSpec(name = "groupCols", portIndices = 0)
@ParamSelectColumnSpec(name = "idCol", portIndices = 0)
@NameCn("分组Dbscan")
@NameEn("Group Dbscan")
public class GroupDbscanBatchOp extends BatchOperator <GroupDbscanBatchOp>
	implements GroupDbscanParams <GroupDbscanBatchOp> {

	private static final long serialVersionUID = 2259660296918166445L;

	public GroupDbscanBatchOp() {
		this(new Params());
	}

	public GroupDbscanBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupDbscanBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		if (!this.getParams().contains(HasPredictionCol.PREDICTION_COL)) {
			this.setPredictionCol("cluster_id");
		}
		if (!this.getParams().contains(GroupDbscanParams.ID_COL)) {
			this.setIdCol(AppendIdBatchOp.appendIdColName);
		}
		final Boolean isOutputVector = getParams().get(IS_OUTPUT_VECTOR);
		final DistanceType distanceType = this.getDistanceType();
		final String latitudeColName = getParams().contains(HasLatitudeCol.LATITUDE_COL) ?
			getParams().get(HasLatitudeCol.LATITUDE_COL) : null;
		final String longitudeColName = getParams().contains(HasLongitudeCol.LONGITUDE_COL) ?
			getParams().get(HasLongitudeCol.LONGITUDE_COL) : null;
		final String[] featureColNames = DistanceType.HAVERSINE.equals(distanceType)
			&& (latitudeColName != null)
			&& (longitudeColName != null) ?
			new String[] {latitudeColName, longitudeColName} : this.get(FEATURE_COLS);

		final int minPoints = getParams().get(MIN_POINTS);
		final Double epsilon = getParams().get(EPSILON);
		final String idCol = getParams().get(ID_COL);
		final String predResultColName = this.getPredictionCol();

		// groupColNames
		final String[] groupColNames = this.getGroupCols();

		Preconditions.checkArgument(distanceType != DistanceType.JACCARD, "Not support Jaccard Distance!");
		FastDistance distance = distanceType.getFastDistance();

		if (distanceType.equals(DistanceType.HAVERSINE)) {
			if (!(featureColNames != null && featureColNames.length == 2)) {
				if ((latitudeColName == null || longitudeColName == null || latitudeColName.isEmpty()
					|| longitudeColName.isEmpty())) {
					throw new RuntimeException("latitudeColName and longitudeColName should be set !");
				}
			}
		} else {
			if ((featureColNames == null || featureColNames.length == 0)) {
				throw new RuntimeException("featureColNames should be set !");
			}
		}

		for (String groupColName : groupColNames) {
			if (TableUtil.findColIndex(featureColNames, groupColName) >= 0) {
				throw new RuntimeException("groupColNames should NOT be included in featureColNames!");
			}
		}

		if (null == idCol || "".equals(idCol)) {
			throw new RuntimeException("idCol column should be set!");
		} else if (TableUtil.findColIndex(featureColNames, idCol) >= 0) {
			throw new RuntimeException("idCol column should NOT be included in featureColNames !");
		} else if (TableUtil.findColIndex(groupColNames, idCol) >= 0) {
			throw new RuntimeException("idCol column should NOT be included in groupColNames !");
		}
		final int dim = featureColNames.length;

		String[] selectedColNames = ArrayUtils.addAll(ArrayUtils.addAll(groupColNames, idCol), featureColNames);
		String[] outputColNames = ArrayUtils.addAll(
			ArrayUtils.addAll(groupColNames, idCol, DbscanConstant.TYPE, predResultColName));
		if (isOutputVector) {
			outputColNames = ArrayUtils.add(outputColNames, DbscanConstant.FEATURE_COL_NAMES);
		} else {
			outputColNames = ArrayUtils.addAll(outputColNames, featureColNames);
		}

		TypeInformation[] outputColTypes = new TypeInformation[outputColNames.length];
		Arrays.fill(outputColTypes, Types.STRING);
		outputColTypes[groupColNames.length + 2] = Types.LONG;
		if (!isOutputVector) {
			Arrays.fill(outputColTypes, groupColNames.length + 3, outputColTypes.length, Types.DOUBLE);
		}

		final TableSchema outputSchema = new TableSchema(outputColNames, outputColTypes);

		final int groupMaxSamples = getGroupMaxSamples();
		final boolean skip = getSkip();

		DataSet <Row> rowDataSet = in.select(selectedColNames).getDataSet()
			.map(new mapToDataVectorSample(dim, groupColNames.length, distance))
			.groupBy(new GroupGeoDbscanBatchOp.SelectGroup())
			.reduceGroup(new GroupGeoDbscanBatchOp.Clustering(epsilon, minPoints, distance, groupMaxSamples, skip))
			.map(new MapToRow(isOutputVector, outputColNames.length, groupColNames.length));

		this.setOutput(rowDataSet, outputSchema);
		return this;
	}

	public static class mapToDataVectorSample extends RichMapFunction <Row, DbscanNewSample> {
		private static final long serialVersionUID = -6733405177253139009L;
		private int dim;
		private int groupColNamesSize;
		private FastDistance distance;

		public mapToDataVectorSample(int dim, int groupColNamesSize, FastDistance distance) {
			this.dim = dim;
			this.groupColNamesSize = groupColNamesSize;
			this.distance = distance;
		}

		@Override
		public DbscanNewSample map(Row row) throws Exception {
			Row keep = new Row(row.getArity() - dim);

			String[] groupColNames = new String[groupColNamesSize];
			for (int i = 0; i < groupColNamesSize; i++) {
				groupColNames[i] = row.getField(i).toString();
				keep.setField(i, groupColNames[i]);
			}
			keep.setField(groupColNamesSize, row.getField(groupColNamesSize).toString());

			double[] values = new double[dim];
			for (int i = 0; i < values.length; i++) {
				values[i] = ((Number) row.getField(i + groupColNamesSize + 1)).doubleValue();
			}
			DenseVector vec = new DenseVector(values);
			FastDistanceVectorData vector = distance.prepareVectorData(Tuple2.of(vec, keep));
			return new DbscanNewSample(vector, groupColNames);
		}
	}

	public static class MapToRow extends RichMapFunction <DbscanNewSample, Row> {
		private static final long serialVersionUID = 4213429941831979236L;
		private int rowArity;
		private int groupColNamesSize;
		private Boolean isOutputVector;

		public MapToRow(Boolean isOutputVector, int rowArity, int groupColNamesSize) {
			this.isOutputVector = isOutputVector;
			this.rowArity = rowArity;
			this.groupColNamesSize = groupColNamesSize;
		}

		@Override
		public Row map(DbscanNewSample value) throws Exception {
			Row row = new Row(rowArity - groupColNamesSize - 1);
			row.setField(0, value.getType().name());
			row.setField(1, value.getClusterId());

			DenseVector v = (DenseVector) value.getVec().getVector();
			if (isOutputVector) {
				row.setField(2, v.toString());
			} else {
				for (int i = 0; i < v.size(); i++) {
					row.setField(i + 2, v.get(i));
				}
			}
			return RowUtil.merge(value.getVec().getRows()[0], row);
		}
	}

}
