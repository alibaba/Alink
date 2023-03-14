package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanConstant;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanNewSample;
import com.alibaba.alink.operator.common.clustering.dbscan.Type;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.distance.HaversineDistance;
import com.alibaba.alink.params.clustering.GroupGeoDbscanParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alibaba.alink.operator.common.clustering.dbscan.Dbscan.UNCLASSIFIED;
import static com.alibaba.alink.operator.common.clustering.dbscan.Dbscan.expandCluster;

/**
 * @author guotao.gt
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT),
})
@ParamSelectColumnSpec(name = "idCol", portIndices = 0)
@ParamSelectColumnSpec(name = "groupCols", portIndices = 0)
@ParamSelectColumnSpec(name = "latitudeCol", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ParamSelectColumnSpec(name = "longitudeCol", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ReservedColsWithFirstInputSpec
@NameCn("分组经纬度Dbscan")
@NameEn("Group Geo Dbscan")

public class GroupGeoDbscanBatchOp extends BatchOperator <GroupGeoDbscanBatchOp>
	implements GroupGeoDbscanParams <GroupGeoDbscanBatchOp> {

	private static final long serialVersionUID = -1650606375272968610L;

	public GroupGeoDbscanBatchOp() {
		this(null);
	}

	public GroupGeoDbscanBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupGeoDbscanBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String latitudeColName = getLatitudeCol();
		final String longitudeColName = getLongitudeCol();
		final int minPoints = getMinPoints();
		final Double epsilon = getParams().get(EPSILON);
		final String idCol = getIdCol();
		final String predResultColName = getPredictionCol();
		final int groupMaxSamples = getGroupMaxSamples();
		final boolean skip = getSkip();
		final String[] keepColNames = getReservedCols();

		FastDistance distance = new HaversineDistance();
		// groupColNames
		final String[] groupColNames = this.getGroupCols();

		if (null == idCol || "".equals(idCol)) {
			throw new RuntimeException("idCol column should be set!");
		} else if (TableUtil.findColIndex(groupColNames, idCol) >= 0) {
			throw new RuntimeException("idCol column should NOT be included in groupColNames !");
		}

		String[] resNames = null == keepColNames ? new String[] {DbscanConstant.TYPE, predResultColName} :
			ArrayUtils.addAll(new String[] {DbscanConstant.TYPE, predResultColName}, keepColNames);
		TypeInformation[] resTypes = null == keepColNames ? new TypeInformation[] {Types.STRING, Types.LONG} :
			ArrayUtils.addAll(new TypeInformation[] {Types.STRING, Types.LONG},
				TableUtil.findColTypesWithAssertAndHint(in.getSchema(), keepColNames));

		String[] columnNames = ArrayUtils.addAll(groupColNames, latitudeColName, longitudeColName);
		columnNames = null == keepColNames ? columnNames : ArrayUtils.addAll(columnNames, keepColNames);

		DataSet <Row> res = in.select(columnNames).getDataSet()
			.map(new mapToDataVectorSample(groupColNames.length, distance))
			.groupBy(new SelectGroup())
			.withPartitioner(new WeightPartitioner())
			.reduceGroup(new Clustering(epsilon, minPoints, distance, groupMaxSamples, skip))
			.map(new MapToRow());
		this.setOutput(res, new TableSchema(resNames, resTypes));

		return this;
	}

	public static class WeightPartitioner implements Partitioner <Integer> {
		private static final long serialVersionUID = -4197634749052990621L;

		@Override
		public int partition(Integer key, int numPartitions) {
			return Math.abs(key) % numPartitions;
		}
	}

	public static class mapToDataVectorSample extends RichMapFunction <Row, DbscanNewSample> {
		private static final long serialVersionUID = -9186022939852072237L;
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

			Row keep = new Row(row.getArity() - groupColNamesSize - 2);
			for (int i = 0; i < keep.getArity(); i++) {
				keep.setField(i, row.getField(groupColNamesSize + 2 + i));
			}

			FastDistanceVectorData data = distance.prepareVectorData(Tuple2.of(vector, keep));

			return new DbscanNewSample(data, groupColNames);
		}
	}

	public static class SelectGroup implements KeySelector <DbscanNewSample, Integer> {
		private static final long serialVersionUID = 6268163376874147254L;

		@Override
		public Integer getKey(DbscanNewSample w) {
			return w.getGroupHashKey();
		}
	}

	public static class Clustering extends RichGroupReduceFunction <DbscanNewSample, DbscanNewSample> {
		private static final long serialVersionUID = 3474119012459738732L;
		private double epsilon;
		private int minPoints;
		private FastDistance baseDistance;
		private int groupMaxSamples;
		private boolean skip;

		public Clustering(double epsilon, int minPoints, FastDistance baseDistance, int groupMaxSamples,
						  boolean skip) {
			this.epsilon = epsilon;
			this.minPoints = minPoints;
			this.baseDistance = baseDistance;
			this.groupMaxSamples = groupMaxSamples;
			this.skip = skip;
		}

		@Override
		public void reduce(Iterable <DbscanNewSample> values, Collector <DbscanNewSample> out) throws Exception {
			int clusterId = 0;
			List <DbscanNewSample> samples = new ArrayList <>();
			for (DbscanNewSample sample : values) {
				samples.add(sample);
			}
			List <DbscanNewSample> abandon = null;
			if (samples.size() >= groupMaxSamples) {
				if (skip) {
					return;
				} else {
					Collections.shuffle(samples);
					List <DbscanNewSample> selected = new ArrayList <>(groupMaxSamples);
					abandon = new ArrayList <>();
					for (int i = 0; i < samples.size(); i++) {
						if (i < groupMaxSamples) {
							selected.add(samples.get(i));
						} else {
							abandon.add(samples.get(i));
						}
					}
					samples = selected;
				}
			}

			for (DbscanNewSample dbscanSample : samples) {
				if (dbscanSample.getClusterId() == UNCLASSIFIED) {
					if (expandCluster(samples, dbscanSample, clusterId, epsilon, minPoints, baseDistance)) {
						clusterId++;
					}
				}
			}

			for (DbscanNewSample dbscanSample : samples) {
				out.collect(dbscanSample);
			}

			//deal with the abandon sample
			if (null != abandon) {
				for (DbscanNewSample sample : abandon) {
					double d = Double.POSITIVE_INFINITY;

					for (DbscanNewSample dbscanNewSample : samples) {
						if (dbscanNewSample.getType().equals(Type.CORE)) {
							double distance = baseDistance.calc(dbscanNewSample.getVec(), sample.getVec()).get(0, 0);
							if (distance < d) {
								sample.setClusterId(dbscanNewSample.getClusterId());
								d = distance;
							}
						}
					}
					if (d > epsilon) {
						sample.setType(Type.NOISE);
						sample.setClusterId(Integer.MIN_VALUE);
					} else {
						sample.setType(Type.LINKED);
					}
					out.collect(sample);
				}
			}
		}
	}

	public static class MapToRow extends RichMapFunction <DbscanNewSample, Row> {
		private static final long serialVersionUID = 5024255660037882136L;

		@Override
		public Row map(DbscanNewSample value) throws Exception {
			return RowUtil.merge(Row.of(value.getType().name(), value.getClusterId()), value.getVec().getRows()[0]);
		}
	}

}
