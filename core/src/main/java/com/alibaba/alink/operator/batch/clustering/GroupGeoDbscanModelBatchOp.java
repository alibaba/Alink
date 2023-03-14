package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanConstant;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanNewSample;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.distance.HaversineDistance;
import com.alibaba.alink.params.clustering.GroupGeoDbscanModelParams;
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
@ParamSelectColumnSpec(name = "groupCols", portIndices = 0)
@ParamSelectColumnSpec(name = "latitudeCol", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ParamSelectColumnSpec(name = "longitudeCol", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@NameCn("分组经纬度Dbscan模型")
@NameEn("Group Geo Dbscan Model")
public class GroupGeoDbscanModelBatchOp extends BatchOperator <GroupGeoDbscanModelBatchOp>
	implements GroupGeoDbscanModelParams <GroupGeoDbscanModelBatchOp> {

	private static final long serialVersionUID = 6424042392598453910L;

	public GroupGeoDbscanModelBatchOp() {
		this(null);
	}

	public GroupGeoDbscanModelBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupGeoDbscanModelBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String latitudeColName = getLatitudeCol();
		final String longitudeColName = getLongitudeCol();
		final int minPoints = getMinPoints();
		final Double epsilon = getParams().get(EPSILON);
		final int groupMaxSamples = getGroupMaxSamples();
		final boolean skip = getSkip();

		FastDistance distance = new HaversineDistance();
		// groupColNames
		final String[] groupColNames = getParams().get(GROUP_COLS);

		if (null == groupColNames) {
			throw new RuntimeException("groupColNames should not be null!");
		}

		String[] resNames = ArrayUtils.addAll(
			new String[] {DbscanConstant.TYPE, "count", latitudeColName, longitudeColName}, groupColNames);
		TypeInformation[] resTypes = ArrayUtils.addAll(
			new TypeInformation[] {AlinkTypes.LONG, AlinkTypes.LONG, AlinkTypes.DOUBLE, AlinkTypes.DOUBLE},
			TableUtil.findColTypesWithAssertAndHint(in.getSchema(), groupColNames));

		String[] columnNames = ArrayUtils.addAll(groupColNames, latitudeColName, longitudeColName);

		DataSet <Row> res = in.select(columnNames).getDataSet()
			.map(new mapToDataVectorSample(groupColNames.length, distance))
			.groupBy(new GroupGeoDbscanBatchOp.SelectGroup())
			.withPartitioner(new GroupGeoDbscanBatchOp.WeightPartitioner())
			.reduceGroup(
				new GroupGeoDbscanBatchOp.Clustering(epsilon, minPoints, distance, groupMaxSamples, skip))
			.groupBy(new SelectGroupCluster())
			.reduceGroup(new getClusteringCenter());

		this.setOutput(res, new TableSchema(resNames, resTypes));

		return this;
	}

	public static class mapToDataVectorSample extends RichMapFunction <Row, DbscanNewSample> {
		private static final long serialVersionUID = -718882540657567670L;
		private int groupColNamesSize;
		private FastDistance distance;

		public mapToDataVectorSample(int groupColNamesSize, FastDistance distance) {
			this.groupColNamesSize = groupColNamesSize;
			this.distance = distance;
		}

		@Override
		public DbscanNewSample map(Row row) throws Exception {
			String[] groupColNames = new String[groupColNamesSize];
			for (int i = 0; i < groupColNamesSize; i++) {
				groupColNames[i] = row.getField(i).toString();
			}
			DenseVector vector = new DenseVector(2);
			vector.set(0, ((Number) row.getField(groupColNamesSize)).doubleValue());
			vector.set(1, ((Number) row.getField(groupColNamesSize + 1)).doubleValue());

			Row keep = new Row(groupColNamesSize);
			for (int i = 0; i < keep.getArity(); i++) {
				keep.setField(i, row.getField(i));
			}
			FastDistanceVectorData vec = distance.prepareVectorData(Tuple2.of(vector, keep));
			return new DbscanNewSample(vec, groupColNames);
		}
	}

	public static class SelectGroupCluster implements KeySelector <DbscanNewSample, Integer> {
		private static final long serialVersionUID = 3160327441213761977L;

		@Override
		public Integer getKey(DbscanNewSample w) {
			String[] key = new String[] {String.valueOf(w.getGroupHashKey()), String.valueOf(w.getClusterId())};
			return new MurmurHash3().arrayHash(key, 0);
		}
	}

	public static class getClusteringCenter implements
		GroupReduceFunction <DbscanNewSample, Row> {
		private static final long serialVersionUID = -1965967509192777460L;

		@Override
		public void reduce(Iterable <DbscanNewSample> values, Collector <Row> out) throws Exception {
			Iterator <DbscanNewSample> iterator = values.iterator();
			long clusterId = Integer.MIN_VALUE;
			Row groupColNames = null;
			long count = 0;
			DenseVector vector = new DenseVector(2);
			if (iterator.hasNext()) {
				DbscanNewSample sample = iterator.next();
				groupColNames = sample.getVec().getRows()[0];
				clusterId = sample.getClusterId();
				vector.plusEqual((DenseVector) sample.getVec().getVector());
				count++;
			}
			// exclude the NOISE
			if (clusterId > Integer.MIN_VALUE) {
				while (iterator.hasNext()) {
					vector.plusEqual((DenseVector) iterator.next().getVec().getVector());
					count++;
				}

				vector.scaleEqual(1.0 / count);
				out.collect(RowUtil.merge(Row.of(clusterId, count, vector.get(0), vector.get(1)), groupColNames));
			}
		}
	}
}
