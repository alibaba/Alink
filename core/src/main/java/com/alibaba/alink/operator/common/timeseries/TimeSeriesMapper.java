package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.Functional.SerializableQuadFunction;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.timeseries.TimeSeriesPredictParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public abstract class TimeSeriesMapper extends Mapper {

	private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesMapper.class);

	private TimeSeries <TimeSeriesMapper> timeSeries;

	public TimeSeriesMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public void open() {
		this.timeSeries = new TimeSeries <>(
			params,
			new Predictor <>(
				this,
				TimeSeriesMapper::predictSingleVar,
				TimeSeriesMapper::predictMultiVar
			)
		);
	}

	@Override
	protected final void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		timeSeries.map(selection, result);
	}

	protected abstract Tuple2 <double[], String> predictSingleVar(
		Timestamp[] historyTimes, double[] historyVals, int predictNum);

	protected abstract Tuple2 <Vector[], String> predictMultiVar(
		Timestamp[] historyTimes, Vector[] historyVals, int predictNum);

	/**
	 * Returns the tuple of selectedCols, resultCols, resultTypes, reservedCols.
	 */
	@Override
	protected final Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		TypeInformation valColType =
			TableUtil.findColType(dataSchema, params.get(TimeSeriesPredictParams.VALUE_COL));

		if (!valColType.equals(AlinkTypes.M_TABLE) && valColType != Types.STRING) {
			throw new AkIllegalDataException("valCol must be mtable or string.");
		}

		return prepareTimeSeriesIoSchema(params);
	}

	static Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareTimeSeriesIoSchema(Params params) {
		ArrayList <String> resultCols = new ArrayList <>();
		ArrayList <TypeInformation <?>> resultTypes = new ArrayList <>();
		resultCols.add(params.get(TimeSeriesPredictParams.PREDICTION_COL));
		resultTypes.add(AlinkTypes.M_TABLE);
		if (params.contains(TimeSeriesPredictParams.PREDICTION_DETAIL_COL)) {
			resultCols.add(params.get(TimeSeriesPredictParams.PREDICTION_DETAIL_COL));
			resultTypes.add(Types.STRING);
		}

		return Tuple4.of(
			new String[] {params.get(TimeSeriesPredictParams.VALUE_COL)},
			resultCols.toArray(new String[0]),
			resultTypes.toArray(new TypeInformation[0]),
			params.get(TimeSeriesPredictParams.RESERVED_COLS)
		);
	}

	static class TimeSeries<M> implements Serializable {
		private final int predictNum;
		private final boolean withPredDetail;

		private final Predictor <M> predictor;

		public TimeSeries(
			Params params,
			Predictor <M> predictor) {
			this.predictNum = params.get(TimeSeriesPredictParams.PREDICT_NUM);
			this.withPredDetail = params.contains(TimeSeriesPredictParams.PREDICTION_DETAIL_COL);
			this.predictor = predictor;
		}

		protected final void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
			Object obj = selection.get(0);

			if (obj instanceof MTable) {
				Tuple2 <MTable, String> t2 = predictor.apply((MTable) obj, this.predictNum);
				result.set(0, t2.f0);
				if (withPredDetail) {
					result.set(1, t2.f1);
				}
			} else if (obj instanceof String) {
				MTable a = MTable.fromJson((String) obj);
				Tuple2 <MTable, String> t2 = predictor.apply(a, this.predictNum);
				result.set(0, t2.f0);
				if (withPredDetail) {
					result.set(1, t2.f1);
				}
			} else {
				result.set(0, null);

				if (withPredDetail) {
					result.set(1, "data is not MTable.");
				}
			}
		}
	}

	static class Predictor<M> implements BiFunction <MTable, Integer, Tuple2 <MTable, String>>, Serializable {
		private final M mapper;
		private final SerializableQuadFunction <
			M, Timestamp[], double[], Integer, Tuple2 <double[], String>> predictSingleVar;
		private final SerializableQuadFunction <
			M, Timestamp[], Vector[], Integer, Tuple2 <Vector[], String>> predictMultiVar;

		public Predictor(
			M mapper,
			SerializableQuadFunction <
				M, Timestamp[], double[], Integer, Tuple2 <double[], String>> predictSingleVar,
			SerializableQuadFunction <
				M, Timestamp[], Vector[], Integer, Tuple2 <Vector[], String>> predictMultiVar) {
			this.mapper = mapper;
			this.predictSingleVar = predictSingleVar;
			this.predictMultiVar = predictMultiVar;
		}

		@Override
		public Tuple2 <MTable, String> apply(MTable historyTS, Integer predictNum) {
			TableSchema schema = historyTS.getSchema();

			String timeCol = null;

			TypeInformation <?>[] colTypes = schema.getFieldTypes();
			for (int i = 0; i < colTypes.length; i++) {
				if (colTypes[i] == Types.SQL_TIMESTAMP) {
					timeCol = schema.getFieldNames()[i];
					break;
				}
			}

			int n = historyTS.getNumRow();

			if (n < 2) {
				return Tuple2.of(null, null);
			}

			int timeIdx = TableUtil.findColIndexWithAssertAndHint(schema, timeCol);

			historyTS.orderBy(timeIdx);

			Timestamp[] historyTimes = new Timestamp[n];
			for (int i = 0; i < n; i++) {
				historyTimes[i] = (Timestamp) historyTS.getEntry(i, timeIdx);
			}

			Timestamp[] predictTimes = getPredictTimes(historyTimes, predictNum);

			String[] valueCols = TableUtil.getNumericCols(schema);

			if (valueCols.length == 1) {
				int valueIdx = TableUtil.findColIndex(schema, valueCols[0]);

				double[] historyValues = new double[n];
				for (int i = 0; i < n; i++) {
					historyValues[i] = ((Number) historyTS.getEntry(i, valueIdx)).doubleValue();
				}

				Tuple2 <double[], String> t2;
				try {
					t2 = predictSingleVar.apply(
						mapper, historyTimes, historyValues, predictNum
					);
				} catch (Throwable ex) {
					LOG.info("Exception caught: ", ex);
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						ex.printStackTrace();
					}
					return Tuple2.of(null, ex.getMessage());
				}

				if (t2.f0 == null) {
					return Tuple2.of(null, null);
				}

				List <Row> rows = new ArrayList <>();
				for (int i = 0; i < t2.f0.length; i++) {
					rows.add(Row.of(predictTimes[i], t2.f0[i]));
				}

				return new Tuple2 <>(
					new MTable(
						rows,
						new String[] {timeCol, valueCols[0]},
						new TypeInformation <?>[] {Types.SQL_TIMESTAMP, Types.DOUBLE}
					),
					t2.f1
				);
			} else {
				int valueIdx = -1;

				for (int i = 0; i < colTypes.length; i++) {
					if (TableUtil.isVector(colTypes[i])) {
						valueIdx = i;
						break;
					}
				}

				if (valueIdx < 0) {
					return Tuple2.of(null, null);
				}

				Vector[] historyValues = new Vector[n];

				for (int i = 0; i < n; i++) {
					historyValues[i] = VectorUtil.getVector(historyTS.getEntry(i, valueIdx));
				}

				Tuple2 <Vector[], String> predictResult;
				try {
					predictResult = predictMultiVar.apply(
						mapper, historyTimes, historyValues, predictNum
					);
				} catch (Throwable ex) {
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						ex.printStackTrace();
					}
					return Tuple2.of(null, ex.getMessage());
				}

				if (predictResult.f0 == null) {
					return Tuple2.of(null, null);
				}

				List <Row> rows = new ArrayList <>();
				for (int i = 0; i < predictResult.f0.length; i++) {
					rows.add(Row.of(predictTimes[i], predictResult.f0[i]));
				}

				return new Tuple2 <>(
					new MTable(
						rows,
						new String[] {timeCol, schema.getFieldNames()[valueIdx]},
						new TypeInformation <?>[] {Types.SQL_TIMESTAMP, AlinkTypes.VECTOR}
					),
					predictResult.f1
				);
			}
		}
	}

	public static Timestamp[] getPredictTimes(Timestamp[] historyTimes, int predictNum) {
		int n = historyTimes.length;

		boolean isWidthEqual = true;

		long diff = historyTimes[n - 1].getTime() - historyTimes[n - 2].getTime();
		for (int i = 0; i < n - 1; i++) {
			if (diff != (historyTimes[i + 1].getTime() - historyTimes[i].getTime())) {
				isWidthEqual = false;
				break;
			}
		}

		Timestamp[] predictTimes = new Timestamp[predictNum];
		if (isWidthEqual) {
			for (int i = 0; i < predictNum; i++) {
				predictTimes[i] = new Timestamp(historyTimes[n - 1].getTime() + diff * i + diff);
			}
		} else {
			LocalDateTime lt1 = historyTimes[n - 1].toLocalDateTime();
			LocalDateTime lt2 = historyTimes[n - 2].toLocalDateTime();

			int yearDiff = lt1.getYear() - lt2.getYear();
			int monthDiff = lt1.getMonthValue() - lt2.getMonthValue();
			int dayDiff = lt1.getDayOfMonth() - lt2.getDayOfMonth();
			int hourDiff = lt1.getHour() - lt2.getHour();
			int minuteDiff = lt1.getMinute() - lt2.getMinute();
			int secondDiff = lt1.getSecond() - lt2.getSecond();
			int nanoDiff = lt1.getNano() - lt2.getNano();

			for (int i = 0; i < predictNum; i++) {
				lt1 = lt1.plusYears(yearDiff)
					.plusMonths(monthDiff)
					.plusDays(dayDiff)
					.plusHours(hourDiff)
					.plusMinutes(minuteDiff)
					.plusSeconds(secondDiff)
					.plusNanos(nanoDiff);
				predictTimes[i] = Timestamp.valueOf(lt1);
			}
		}

		return predictTimes;
	}

}