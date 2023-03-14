package com.alibaba.alink.operator.batch.clustering;

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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.LocalKMeans;
import com.alibaba.alink.operator.common.clustering.common.Sample;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.params.clustering.GroupKMeansParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT),
})
@ParamSelectColumnSpec(name = "featureCols", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ParamSelectColumnSpec(name = "groupCols", portIndices = 0)
@ParamSelectColumnSpec(name = "idCol", portIndices = 0)
@NameCn("分组EM")
@NameEn("Group EM")
public final class GroupEmBatchOp extends BatchOperator<GroupEmBatchOp>
	implements GroupKMeansParams <GroupEmBatchOp> {

	private static final long serialVersionUID = 2403292854593151120L;

	public GroupEmBatchOp() {
		super(null);
	}

	public GroupEmBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupEmBatchOp linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);

		if(!this.getParams().contains(HasPredictionCol.PREDICTION_COL)){
			this.setPredictionCol("cluster_id");
		}
		final String[] featureColNames = (this.getParams().contains(FEATURE_COLS)
			&& this.getFeatureCols() != null
			&& this.getFeatureCols().length > 0) ?
			this.getFeatureCols() : TableUtil.getNumericCols(in.getSchema());
		final int k = this.getK();
		final double epsilon = this.getEpsilon();
		final int maxIter = this.getMaxIter();
		final DistanceType distanceType = getDistanceType();
		final String[] groupColNames = this.getGroupCols();
		final String idCol = this.getIdCol();
		final String predResultColName = this.getPredictionCol();

		ContinuousDistance distance = distanceType.getFastDistance();

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

		StringBuilder sbd = new StringBuilder();
		for (String groupColName : groupColNames) {
			sbd.append("cast(`").append(groupColName).append("` as VARCHAR) as `").append(groupColName).append("`, ");
		}
		sbd.append("cast(`").append(idCol).append("` as VARCHAR) as `").append(idCol).append("`, ");
		for (int i = 0; i < featureColNames.length; i++) {
			if (i > 0) {
				sbd.append(", ");
			}
			sbd.append("cast(`")
				.append(featureColNames[i])
				.append("` as double) as `")
				.append(featureColNames[i])
				.append("`");
		}

		final int dim = featureColNames.length;

		List <String> columnNames = new ArrayList <>();
		for (String groupColName : groupColNames) {
			columnNames.add(groupColName);
		}
		columnNames.add(idCol);
		columnNames.add(predResultColName);
		for (String col : featureColNames) {
			columnNames.add(col);
		}

		List <TypeInformation> columnTypes = new ArrayList <>();
		for (String groupColName : groupColNames) {
			columnTypes.add(Types.STRING());
		}
		columnTypes.add(Types.STRING());
		columnTypes.add(Types.LONG());
		for (String col : featureColNames) {
			columnTypes.add(Types.DOUBLE());
		}

		final TableSchema outputSchema = new TableSchema(
			columnNames.toArray(new String[columnNames.size()]),
			columnTypes.toArray(new TypeInformation[columnTypes.size()])
		);

		try {
			DataSet <Row> rowDataSet = in.select(sbd.toString()).getDataSet()
				.map(new mapToDataSample(dim, groupColNames.length))
				.groupBy(new SelectGroup())
				.reduceGroup(new Clustering(k, epsilon, maxIter, dim, distance))
				.map(new MapToRow(columnNames.size(), groupColNames.length));

			this.setOutput(rowDataSet, outputSchema);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException(ex);
		}

		return this;
	}

	public static class mapToDataSample implements MapFunction <Row, Sample> {
		private static final long serialVersionUID = -1252574650762802849L;
		private int dim;
		private int groupColNamesSize;

		public mapToDataSample(int dim, int groupColNamesSize) {
			this.dim = dim;
			this.groupColNamesSize = groupColNamesSize;
		}

		@Override
		public Sample map(Row row) throws Exception {
			List <String> groupColNames = new ArrayList <>();
			for (int i = 0; i < groupColNamesSize; i++) {
				if(null == row.getField(i)){
					throw new RuntimeException("There is NULL value in group col!");
				}
				groupColNames.add((String) row.getField(i));
			}

			if(null == row.getField(groupColNamesSize)){
				throw new RuntimeException("There is NULL value in id col!");
			}
			String idColValue = (String) row.getField(groupColNamesSize);

			double[] values = new double[dim];
			for (int i = 0; i < values.length; i++) {
				if(null == row.getField(i + groupColNamesSize + 1)){
					throw new RuntimeException("There is NULL value in value col!");
				}
				values[i] = (Double) row.getField(i + groupColNamesSize + 1);
			}
			return new Sample(idColValue, new DenseVector(values), -1,
				groupColNames.toArray(new String[groupColNamesSize]));
		}
	}

	public static class Clustering implements GroupReduceFunction <Sample, Sample> {
		private static final long serialVersionUID = -6401148777324895859L;
		private int k;
		private double epsilon;
		private ContinuousDistance distance;
		private int maxIter;
		private int dim;

		public Clustering(int k, double epsilon, int maxIter, int dim, ContinuousDistance distance) {
			this.epsilon = epsilon;
			this.k = k;
			this.distance = distance;
			this.maxIter = maxIter;
			this.dim = dim;
		}

		@Override
		public void reduce(Iterable <Sample> values, Collector <Sample> out) throws Exception {
			LocalKMeans.clustering(values, out, k, epsilon, maxIter, distance);
		}
	}

	public static class MapToRow implements MapFunction <Sample, Row> {
		private static final long serialVersionUID = 5045205789035382392L;
		private int rowArity;
		private int groupColNamesSize;

		public MapToRow(int rowArity, int groupColNamesSize) {
			this.rowArity = rowArity;
			this.groupColNamesSize = groupColNamesSize;
		}

		@Override
		public Row map(Sample value) throws Exception {
			Row row = new Row(rowArity);
			DenseVector denseVector = value.getVector();
			for (int i = 0; i < groupColNamesSize; i++) {
				row.setField(i, value.getGroupColNames()[i]);
			}
			row.setField(groupColNamesSize, value.getSampleId());
			row.setField(groupColNamesSize + 1, value.getClusterId());
			for (int i = 0; i < denseVector.size(); i++) {
				row.setField(i + groupColNamesSize + 2, denseVector.get(i));
			}
			return row;
		}
	}

	public class SelectGroup implements KeySelector <Sample, String> {
		private static final long serialVersionUID = 4582197026301874450L;

		@Override
		public String getKey(Sample w) {
			return w.getGroupColNamesString();
		}
	}

}
