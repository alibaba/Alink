package com.alibaba.alink.operator.batch.statistics.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.DenseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.VectorSummarizerUtil;
import com.alibaba.alink.operator.common.statistics.statistics.SrtUtil;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.operator.common.statistics.statistics.WindowTable;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;

/**
 * Util for batch statistical calculation.
 */
public class StatisticsHelper {
	/**
	 * Transform cols or vector col to vector and compute summary.
	 * it deals with table which only have number cols and not has missing value.
	 * It returns tuple2, f0 is data, f1 is summary. data f0 is vector, data f1 is row which is reserved cols.
	 */
	public static Tuple2 <DataSet <Tuple2 <Vector, Row>>, DataSet <BaseVectorSummary>> summaryHelper(
		BatchOperator <?> in,
		String[] selectedColNames,
		String vectorColName,
		String[] reservedColNames) {

		DataSet <Tuple2 <Vector, Row>> data = transformToVector(in, selectedColNames, vectorColName, reservedColNames);

		DataSet <Vector> vectorDataSet = data
			.map(new MapFunction <Tuple2 <Vector, Row>, Vector>() {
				private static final long serialVersionUID = -1465299071490594701L;

				@Override
				public Vector map(Tuple2 <Vector, Row> tuple2) {
					return tuple2.f0;
				}
			});

		return Tuple2.of(data, summary(vectorDataSet));
	}

	/**
	 * Transform to vector without reservedColNames and compute summary.
	 * it deals with table which only have number cols and has no missing value.
	 */
	public static Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> summaryHelper(BatchOperator <?> in,
																					   String[] selectedColNames,
																					   String vectorColName) {
		DataSet <Vector> data = transformToVector(in, selectedColNames, vectorColName);
		return Tuple2.of(data, summary(data));
	}

	/**
	 * table summary, selectedColNames must be set.
	 */
	public static DataSet <TableSummary> summary(BatchOperator <?> in, String[] selectedColNames) {
		return summarizer(in, selectedColNames, false)
			.map(new MapFunction <TableSummarizer, TableSummary>() {
				private static final long serialVersionUID = 8876418210242735806L;

				@Override
				public TableSummary map(TableSummarizer summarizer) {
					return summarizer.toSummary();
				}
			}).name("toSummary");
	}

	/**
	 * vector stat, selectedColName must be set.
	 */
	public static DataSet <BaseVectorSummary> vectorSummary(BatchOperator <?> in, String selectedColName) {
		return vectorSummarizer(in, selectedColName, false)
			.map(new MapFunction <BaseVectorSummarizer, BaseVectorSummary>() {
				private static final long serialVersionUID = -6426572658193278213L;

				@Override
				public BaseVectorSummary map(BaseVectorSummarizer summarizer) {
					return summarizer.toSummary();
				}
			}).name("toSummary");
	}

	/**
	 * given vector dataSet, return vector summary.
	 */
	public static DataSet <BaseVectorSummary> summary(DataSet <Vector> data) {
		return summarizer(data, false)
			.map(new MapFunction <BaseVectorSummarizer, BaseVectorSummary>() {
				private static final long serialVersionUID = -2082435777065038687L;

				@Override
				public BaseVectorSummary map(BaseVectorSummarizer summarizer) {
					return summarizer.toSummary();
				}
			}).name("toSummary");
	}

	/**
	 * calculate correlation. result is tuple2, f0 is summary, f1 is correlation.
	 */
	public static DataSet <Tuple2 <TableSummary, CorrelationResult>> pearsonCorrelation(BatchOperator <?> in,
																						String[] selectedColNames) {
		return summarizer(in, selectedColNames, true)
			.map(new MapFunction <TableSummarizer, Tuple2 <TableSummary, CorrelationResult>>() {
				private static final long serialVersionUID = -5757257375097509823L;

				@Override
				public Tuple2 <TableSummary, CorrelationResult> map(TableSummarizer summarizer) {
					return Tuple2.of(summarizer.toSummary(), summarizer.correlation());
				}
			});
	}

	/**
	 * calculate correlation. result is tuple2, f0 is summary, f1 is correlation.
	 */
	public static DataSet <Tuple2 <BaseVectorSummary, CorrelationResult>> vectorPearsonCorrelation(
		BatchOperator <?> in,
		String selectedColName) {

		return vectorSummarizer(in, selectedColName, true)
			.map(new MapFunction <BaseVectorSummarizer, Tuple2 <BaseVectorSummary, CorrelationResult>>() {
				private static final long serialVersionUID = -1745468840082156193L;

				@Override
				public Tuple2 <BaseVectorSummary, CorrelationResult> map(BaseVectorSummarizer summarizer) {
					return Tuple2.of(summarizer.toSummary(), summarizer.correlation());
				}
			});
	}

	/**
	 * transform to vector.
	 */
	public static DataSet <Vector> transformToVector(BatchOperator <?> in,
													 String[] selectedColNames,
													 String vectorColName) {

		checkSimpleStatParameter(in, selectedColNames, vectorColName, null);

		if (selectedColNames != null && selectedColNames.length != 0) {
			int[] selectedColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), selectedColNames);
			return in.getDataSet().map(new ColsToVectorWithoutReservedColsMap(selectedColIndices));
		} else {
			int selectColIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName);
			return in.getDataSet().map(new VectorCoToVectorWithoutReservedColsMap(selectColIndex));
		}
	}

	/**
	 * transform to vector, result is tuple2, f0 is vector, f1 is reserved cols.
	 */
	public static DataSet <Tuple2 <Vector, Row>> transformToVector(BatchOperator <?> in,
																   String[] selectedColNames,
																   String vectorColName,
																   String[] reservedColNames) {

		checkSimpleStatParameter(in, selectedColNames, vectorColName, reservedColNames);

		int[] reservedColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), reservedColNames);

		if (selectedColNames != null && selectedColNames.length != 0) {
			int[] selectedColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), selectedColNames);
			return in.getDataSet()
				.map(new ColsToVectorWithReservedColsMap(selectedColIndices, reservedColIndices))
				.name("transform_data");
		} else {
			int vectorColIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName);
			return in.getDataSet()
				.map(new VectorColToVectorWithReservedColsMap(vectorColIndex, reservedColIndices))
				.name("transform_data");
		}
	}

	/**
	 * Transform vector or table columns to table columns.
	 * selectedCols and vectorCol only be one non-empty, reserved cols can be empty.
	 * <p>
	 * If selected cols is not null, it will combine selected cols and reserved cols,
	 * and selected cols will transform to double type.
	 * <p>
	 * If vector col is not null,  it will split vector to cols and combines with reserved cols.
	 * <p>
	 * If selected cols and vector col both set, it will use vector col.
	 */
	public static DataSet <Row> transformToColumns(BatchOperator <?> in,
												   String[] selectedColNames,
												   String vectorColName,
												   String[] reservedColNames) {
		checkSimpleStatParameter(in, selectedColNames, vectorColName, reservedColNames);

		int[] reservedColIndices = null;
		if (reservedColNames != null) {
			reservedColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), reservedColNames);
		}

		if (selectedColNames != null && selectedColNames.length != 0) {
			int[] selectedColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getSchema(), selectedColNames);
			return in.getDataSet().map(new ColsToDoubleColsMap(selectedColIndices, reservedColIndices));
		}

		if (vectorColName != null) {
			int vectorColIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName);
			return in.getDataSet().map(new VectorColToTableMap(vectorColIndex, reservedColIndices));
		}

		throw new AkIllegalOperatorParameterException("selectedColName and vectorColName must be set one only.");
	}

	/**
	 * check parameters is invalid.
	 */
	private static void checkSimpleStatParameter(BatchOperator <?> in,
												 String[] selectedColNames,
												 String vectorColName,
												 String[] reservedColNames) {
		if (selectedColNames != null && selectedColNames.length != 0 && vectorColName != null) {
			throw new AkIllegalOperatorParameterException("selectedColName and vectorColName must be set one only.");
		}

		TableUtil.assertSelectedColExist(in.getColNames(), selectedColNames);
		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		TableUtil.assertSelectedColExist(in.getColNames(), vectorColName);
		TableUtil.assertVectorCols(in.getSchema(), vectorColName);

		TableUtil.assertSelectedColExist(in.getColNames(), reservedColNames);
	}

	/**
	 * table stat
	 */
	private static DataSet <TableSummarizer> summarizer(BatchOperator <?> in,
														String[] selectedColNames,
														boolean calculateOuterProduct) {
		if (selectedColNames == null || selectedColNames.length == 0) {
			throw new AkIllegalOperatorParameterException("selectedColNames must be set.");
		}

		in = Preprocessing.select(in, selectedColNames);

		return summarizer(in.getDataSet(), in.getSchema(), calculateOuterProduct);
	}

	/**
	 * vector stat
	 */
	private static DataSet <BaseVectorSummarizer> vectorSummarizer(BatchOperator <?> in,
																   String selectedColName,
																   boolean calculateOuterProduct) {
		TableUtil.assertSelectedColExist(in.getColNames(), selectedColName);

		DataSet <Vector> data = transformToVector(in, null, selectedColName);
		return summarizer(data, calculateOuterProduct);
	}

	/**
	 * given vector dataSet, return vector summary. if outerProduct is true, calculate covariance.
	 */
	public static DataSet <BaseVectorSummarizer> summarizer(DataSet <Vector> data, boolean bCov) {
		return data
			.mapPartition(new VectorSummarizerPartition(bCov)).name("summarizer_map")
			.reduce(new ReduceFunction <BaseVectorSummarizer>() {
				private static final long serialVersionUID = 5993118429985684366L;

				@Override
				public BaseVectorSummarizer reduce(BaseVectorSummarizer value1, BaseVectorSummarizer value2) {
					return VectorSummarizerUtil.merge(value1, value2);
				}
			})
			.name("summarizer_reduce");
	}

	/**
	 * given data, return summary. numberIndices is the indices of cols which are number type in selected cols.
	 */
	private static DataSet <TableSummarizer> summarizer(DataSet <Row> data, TableSchema tableSchema, boolean bCov) {
		return data
			.mapPartition(new TableSummarizerPartition(tableSchema, bCov))
			.reduce(new ReduceFunction <TableSummarizer>() {
				private static final long serialVersionUID = 964700189305139868L;

				@Override
				public TableSummarizer reduce(TableSummarizer left, TableSummarizer right) {
					return TableSummarizer.merge(left, right);
				}
			});
	}

	public static DataSet <SummaryResultTable> getSRT(BatchOperator<?> in, HasStatLevel_L1.StatLevel statLevel) {
		return in.getDataSet()
			.mapPartition(new SetPartitionBasicStat(in.getSchema(), statLevel))
			.reduce(new ReduceFunction <SummaryResultTable>() {
				private static final long serialVersionUID = 6050967884386340459L;

				@Override
				public SummaryResultTable reduce(SummaryResultTable a, SummaryResultTable b) {
					if (null == a) {
						return b;
					} else if (null == b) {
						return a;
					} else {
						return SummaryResultTable.combine(a, b);
					}
				}
			});
	}

	/**
	 * MapFunction for Transform cols to vector. result it tuple, f0 is vector, f1 is reserved cols.
	 */
	private static class ColsToVectorWithReservedColsMap implements MapFunction <Row, Tuple2 <Vector, Row>> {
		private static final long serialVersionUID = -7292044433828115396L;
		private final int[] selectedColIndices;
		private final int[] reservedColIndices;

		ColsToVectorWithReservedColsMap(int[] selectedColIndices, int[] reservedColIndices) {
			this.selectedColIndices = selectedColIndices;
			this.reservedColIndices = reservedColIndices;
		}

		@Override
		public Tuple2 <Vector, Row> map(Row in) throws Exception {
			DenseVector dv = new DenseVector(selectedColIndices.length);
			for (int i = 0; i < selectedColIndices.length; i++) {
				dv.set(i, ((Number) in.getField(this.selectedColIndices[i])).doubleValue());
			}

			Row out = new Row(reservedColIndices.length);
			for (int i = 0; i < this.reservedColIndices.length; ++i) {
				out.setField(i, in.getField(this.reservedColIndices[i]));
			}
			return new Tuple2 <>(dv, out);
		}
	}

	/**
	 * MapFunction for Transform cols to vector.
	 */
	private static class ColsToVectorWithoutReservedColsMap implements MapFunction <Row, Vector> {
		private static final long serialVersionUID = -8479361651447801687L;
		private final int[] selectedColIndices;

		ColsToVectorWithoutReservedColsMap(int[] selectedColIndices) {
			this.selectedColIndices = selectedColIndices;
		}

		@Override
		public Vector map(Row in) throws Exception {
			DenseVector dv = new DenseVector(selectedColIndices.length);
			double[] data = dv.getData();
			for (int i = 0; i < selectedColIndices.length; i++) {
				Object obj = in.getField(this.selectedColIndices[i]);
				if (obj instanceof Number) {
					data[i] = ((Number) obj).doubleValue();
				}
			}

			return dv;
		}
	}

	/**
	 * MapFunction for vector col transform to vector. result is tuple2, f0 is vector, f1 is reserved cols.
	 */
	private static class VectorColToVectorWithReservedColsMap implements MapFunction <Row, Tuple2 <Vector, Row>> {
		private static final long serialVersionUID = -3222351920500305742L;
		private final int vectorColIndex;
		private final int[] reservedColIndices;

		VectorColToVectorWithReservedColsMap(int vectorColIndex, int[] reservedColIndices) {
			this.vectorColIndex = vectorColIndex;
			this.reservedColIndices = reservedColIndices;
		}

		@Override
		public Tuple2 <Vector, Row> map(Row in) throws Exception {
			Vector vec = VectorUtil.getVector(in.getField(vectorColIndex));
			if (vec == null) {
				throw new AkIllegalDataException("input vector is null");
			}
			Row out = new Row(reservedColIndices.length);
			for (int i = 0; i < this.reservedColIndices.length; ++i) {
				out.setField(i, in.getField(this.reservedColIndices[i]));
			}
			return Tuple2.of(vec, out);
		}
	}

	/**
	 * MapFunction for vector col transform to vector.
	 */
	private static class VectorCoToVectorWithoutReservedColsMap implements MapFunction <Row, Vector> {
		private static final long serialVersionUID = -6220416346174572528L;
		private final int vectorColIndex;

		VectorCoToVectorWithoutReservedColsMap(int vectorColIndex) {
			this.vectorColIndex = vectorColIndex;
		}

		@Override
		public Vector map(Row in) throws Exception {
			return VectorUtil.getVector(in.getField(vectorColIndex));
		}
	}

	/**
	 * Transform vector col to table columns.
	 */
	private static class VectorColToTableMap implements MapFunction <Row, Row> {

		private static final long serialVersionUID = 4663377654190680333L;
		/**
		 * vector col index.
		 */
		private final int vectorColIndex;
		/**
		 * reserved col indices.
		 */
		private final int[] reservedColIndices;

		VectorColToTableMap(int vectorColIndex, int[] reservedColIndices) {
			this.vectorColIndex = vectorColIndex;
			this.reservedColIndices = null == reservedColIndices ? new int[0] : reservedColIndices;
		}

		@Override
		public Row map(Row in) throws Exception {
			Row out;

			Vector vector = VectorUtil.getVector(in.getField(vectorColIndex));

			DenseVector feature;

			if (vector instanceof DenseVector) {
				feature = (DenseVector) vector;
			} else {
				feature = ((SparseVector) vector).toDenseVector();
			}

			if (feature.getData() != null) {
				out = new Row(feature.size() + reservedColIndices.length);
				for (int i = 0; i < feature.size(); i++) {
					out.setField(i, feature.get(i));
				}
				for (int i = 0; i < reservedColIndices.length; i++) {
					out.setField(i + feature.size(), in.getField(reservedColIndices[i]));
				}
			} else {
				out = new Row(reservedColIndices.length);
				for (int i = 0; i < reservedColIndices.length; i++) {
					out.setField(i, in.getField(reservedColIndices[i]));
				}
			}

			return out;
		}
	}

	/**
	 * MapFunction for cols.
	 */
	private static class ColsToDoubleColsMap implements MapFunction <Row, Row> {
		private static final long serialVersionUID = 2021889928304298454L;
		private final int[] selectedColIndices;
		private final int[] reservedColIndices;

		ColsToDoubleColsMap(int[] selectedColIndices, int[] reservedColIndices) {
			this.selectedColIndices = selectedColIndices;
			this.reservedColIndices = null == reservedColIndices ? new int[0] : reservedColIndices;
		}

		@Override
		public Row map(Row in) throws Exception {
			//table cols and reserved cols, table cols will be transformed to double type.
			Row out = new Row(selectedColIndices.length + reservedColIndices.length);
			for (int i = 0; i < this.selectedColIndices.length; ++i) {
				out.setField(i, ((Number) in.getField(this.selectedColIndices[i])).doubleValue());
			}
			for (int i = 0; i < reservedColIndices.length; i++) {
				out.setField(i + selectedColIndices.length, in.getField(reservedColIndices[i]));
			}
			return out;
		}
	}

	/**
	 * It is table summary partition of one worker, will merge result later.
	 */
	private static class TableSummarizerPartition implements MapPartitionFunction <Row, TableSummarizer> {
		private static final long serialVersionUID = -1625614901816383530L;
		private final boolean outerProduct;
		private final String[] colNames;
		private final TypeInformation <?>[] colTypes;

		TableSummarizerPartition(TableSchema tableSchema, boolean outerProduct) {
			this.outerProduct = outerProduct;
			this.colNames = tableSchema.getFieldNames();
			this.colTypes = tableSchema.getFieldTypes();
		}

		@Override
		public void mapPartition(Iterable <Row> iterable, Collector <TableSummarizer> collector) throws Exception {
			TableSummarizer srt = new TableSummarizer(new TableSchema(colNames, colTypes), outerProduct);
			for (Row sv : iterable) {
				srt = (TableSummarizer) srt.visit(sv);
			}

			collector.collect(srt);
		}
	}

	/**
	 * It is vector summary partition of one worker, will merge result later.
	 */
	public static class VectorSummarizerPartition implements MapPartitionFunction <Vector, BaseVectorSummarizer> {
		private static final long serialVersionUID = 1065284716432882945L;
		private final boolean outerProduct;

		public VectorSummarizerPartition(boolean outerProduct) {
			this.outerProduct = outerProduct;
		}

		@Override
		public void mapPartition(Iterable <Vector> iterable, Collector <BaseVectorSummarizer> collector)
			throws Exception {
			BaseVectorSummarizer srt = new DenseVectorSummarizer(outerProduct);
			for (Vector sv : iterable) {
				srt = srt.visit(sv);
			}

			collector.collect(srt);
		}
	}

	/**
	 * @author yangxu
	 */
	public static class SetPartitionBasicStat implements MapPartitionFunction <Row, SummaryResultTable> {

		private static final long serialVersionUID = -5607403479996476267L;
		private String[] colNames;
		private Class[] colTypes;
		private HasStatLevel_L1.StatLevel statLevel;
		private String[] selectedColNames = null;

		public SetPartitionBasicStat(TableSchema schema) {
			this(schema, HasStatLevel_L1.StatLevel.L1);
		}

		/**
		 * @param schema
		 * @param statLevel: L1,L2,L3: 默认是L1
		 *                   L1 has basic statistic;
		 *                   L2 has simple statistic and cov/corr;
		 *                   L3 has simple statistic, cov/corr, histogram, freq, topk, bottomk;
		 */
		public SetPartitionBasicStat(TableSchema schema, HasStatLevel_L1.StatLevel statLevel) {
			this.colNames = schema.getFieldNames();
			int n = this.colNames.length;
			this.colTypes = new Class[n];
			for (int i = 0; i < n; i++) {
				colTypes[i] = schema.getFieldTypes()[i].getTypeClass();
			}
			this.statLevel = statLevel;
			this.selectedColNames = this.colNames;
		}

		/**
		 * @param schema
		 * @param statLevel:       L1,L2,L3: 默认是L1
		 *                         L1 has basic statistic;
		 *                         L2 has simple statistic and cov/corr;
		 *                         L3 has simple statistic, cov/corr, histogram, freq, topk, bottomk;
		 * @param selectedColNames
		 */
		public SetPartitionBasicStat(TableSchema schema, String[] selectedColNames, HasStatLevel_L1.StatLevel statLevel) {
			this.colNames = schema.getFieldNames();
			int n = this.colNames.length;
			this.colTypes = new Class[n];
			for (int i = 0; i < n; i++) {
				colTypes[i] = schema.getFieldTypes()[i].getTypeClass();
			}
			this.statLevel = statLevel;
			this.selectedColNames = selectedColNames;
		}

		@Override
		public void mapPartition(Iterable <Row> itrbl, Collector <SummaryResultTable> clctr) throws Exception {
			WindowTable wt = new WindowTable(this.colNames, this.colTypes, itrbl);
			SummaryResultTable srt = SrtUtil.batchSummary(wt, this.selectedColNames, 10, 10, 1000, 10, this.statLevel);
			if (srt != null) {
				clctr.collect(srt);
			}
		}

	}
}