package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
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
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanCenter;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanConstant;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanNewSample;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.params.clustering.GroupDbscanModelParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import org.apache.commons.lang3.ArrayUtils;
import scala.util.hashing.MurmurHash3;

import java.util.Iterator;

/**
 * @author guotao.gt
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.MODEL_INFO),
})
@ReservedColsWithSecondInputSpec
@ParamSelectColumnSpec(name = "featureCols", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ParamSelectColumnSpec(name = "groupCols", portIndices = 0)
@NameCn("分组Dbscan模型")
@NameEn("Group Dbscan Model")
public final class GroupDbscanModelBatchOp extends BatchOperator <GroupDbscanModelBatchOp>
	implements GroupDbscanModelParams <GroupDbscanModelBatchOp> {

	private static final long serialVersionUID = 5788206252024914272L;

	public GroupDbscanModelBatchOp() {
		this(new Params());
	}

	public GroupDbscanModelBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupDbscanModelBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		if (!this.getParams().contains(HasPredictionCol.PREDICTION_COL)) {
			this.setPredictionCol("cluster_id");
		}
		final DistanceType distanceType = getDistanceType();
		final String[] featureColNames = this.getParams().get(FEATURE_COLS);

		final int minPoints = getParams().get(MIN_POINTS);
		final Double epsilon = getParams().get(EPSILON);
		final String predResultColName = this.getPredictionCol();

		// groupColNames
		final String[] groupColNames = getParams().get(GROUP_COLS);

		for (String groupColName : groupColNames) {
			if (TableUtil.findColIndex(featureColNames, groupColName) >= 0) {
				throw new RuntimeException("groupColNames should NOT be included in featureColNames!");
			}
		}

		String[] selectedColNames = ArrayUtils.addAll(groupColNames, featureColNames);

		Preconditions.checkArgument(distanceType != DistanceType.JACCARD, "Not support %s!", distanceType.name());
		FastDistance distance = distanceType.getFastDistance();

		final int dim = featureColNames.length;

		String[] outputColNames = ArrayUtils.addAll(
			ArrayUtils.addAll(groupColNames, predResultColName, DbscanConstant.COUNT), featureColNames);
		TypeInformation[] outputColTypes = ArrayUtils.addAll(
			ArrayUtils.addAll(TableUtil.findColTypesWithAssert(in.getSchema(), groupColNames), Types.LONG,
				Types.LONG), TableUtil.findColTypesWithAssertAndHint(in.getSchema(), featureColNames));

		final TableSchema outputSchema = new TableSchema(outputColNames, outputColTypes);

		final int groupMaxSamples = getGroupMaxSamples();
		final boolean skip = getSkip();

		DataSet <Row> rowDataSet = in.select(selectedColNames).getDataSet()
			.map(new mapToDataSample(dim, groupColNames.length, distance))
			.groupBy(new GroupGeoDbscanBatchOp.SelectGroup())
			.reduceGroup(new GroupGeoDbscanBatchOp.Clustering(epsilon, minPoints, distance, groupMaxSamples, skip))
			.groupBy(new SelectGroupAndClusterID())
			.reduceGroup(new getClusteringCenter(dim, distanceType))
			.map(new MapToRow(outputColNames.length, groupColNames.length));

		this.setOutput(rowDataSet, outputSchema);

		return this;
	}

	public static class mapToDataSample implements MapFunction <Row, DbscanNewSample> {
		private static final long serialVersionUID = 1491814462425438888L;
		private int dim;
		private int groupColNamesSize;
		private FastDistance distance;

		public mapToDataSample(int dim, int groupColNamesSize, FastDistance distance) {
			this.dim = dim;
			this.groupColNamesSize = groupColNamesSize;
			this.distance = distance;
		}

		@Override
		public DbscanNewSample map(Row row) throws Exception {
			String[] groupColNames = new String[groupColNamesSize];
			for (int i = 0; i < groupColNamesSize; i++) {
				groupColNames[i] = row.getField(i).toString();
			}

			double[] values = new double[dim];
			for (int i = 0; i < values.length; i++) {
				values[i] = (Double) row.getField(i + groupColNamesSize);
			}
			DenseVector vec = new DenseVector(values);

			Row keep = new Row(groupColNamesSize);
			for (int i = 0; i < keep.getArity(); i++) {
				keep.setField(i, row.getField(i));
			}
			FastDistanceVectorData vector = distance.prepareVectorData(Tuple2.of(vec, keep));

			return new DbscanNewSample(vector, groupColNames);
		}
	}

	public static class getClusteringCenter
		implements GroupReduceFunction <DbscanNewSample, DbscanCenter <DenseVector>> {
		private static final long serialVersionUID = 6317085010066332931L;
		private int dim;
		private DistanceType distanceType;

		public getClusteringCenter(int dim, DistanceType distanceType) {
			this.dim = dim;
			this.distanceType = distanceType;
		}

		@Override
		public void reduce(Iterable <DbscanNewSample> values, Collector <DbscanCenter <DenseVector>> out)
			throws Exception {
			Iterator <DbscanNewSample> iterator = values.iterator();
			long clusterId = 0;
			Row groupColNames = null;
			int count = 0;
			DenseVector vector = new DenseVector(dim);
			if (iterator.hasNext()) {
				DbscanNewSample sample = iterator.next();
				clusterId = sample.getClusterId();
				groupColNames = sample.getVec().getRows()[0];
				vector.plusEqual(sample.getVec().getVector());
				count++;
			}
			// exclude the NOISE
			if (clusterId > Integer.MIN_VALUE) {
				while (iterator.hasNext()) {
					vector.plusEqual(iterator.next().getVec().getVector());
					count++;
				}

				vector.scaleEqual(1.0 / count);

				DbscanCenter <DenseVector> dbscanCenter = new DbscanCenter <DenseVector>(groupColNames, clusterId,
					count, vector);
				out.collect(dbscanCenter);
			}
		}
	}

	public static class MapToRow implements MapFunction <DbscanCenter <DenseVector>, Row> {
		private static final long serialVersionUID = -5480092592936407825L;
		private int rowArity;
		private int groupColNamesSize;

		public MapToRow(int rowArity, int groupColNamesSize) {
			this.rowArity = rowArity;
			this.groupColNamesSize = groupColNamesSize;
		}

		@Override
		public Row map(DbscanCenter <DenseVector> value) throws Exception {
			Row row = new Row(rowArity - groupColNamesSize);
			DenseVector denseVector = value.getValue();
			row.setField(0, value.getClusterId());
			row.setField(1, value.getCount());
			for (int i = 0; i < denseVector.size(); i++) {
				row.setField(i + 2, denseVector.get(i));
			}
			return RowUtil.merge(value.getGroupColNames(), row);
		}
	}

	public class SelectGroupAndClusterID implements KeySelector <DbscanNewSample, Integer> {
		private static final long serialVersionUID = -8204871256389225863L;

		@Override
		public Integer getKey(DbscanNewSample w) {
			return new MurmurHash3().arrayHash(new Integer[] {(int) w.getClusterId(), w.getGroupHashKey()}, 0);
		}
	}

}
