package com.alibaba.alink.operator.local.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.feature.OneHotModelInfo;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.feature.HasEnableElse;
import com.alibaba.alink.params.feature.OneHotTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * One-hot maps a serial of columns of category indices to a column of sparse binary vector. It will produce a model of
 * one hot, and then it can transform data to binary format using this model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@SelectedColsWithFirstInputSpec
@NameCn("独热编码训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.OneHotEncoder")
public final class OneHotTrainLocalOp extends LocalOperator <OneHotTrainLocalOp>
	implements OneHotTrainParams <OneHotTrainLocalOp>,
	WithModelInfoLocalOp <OneHotModelInfo, OneHotTrainLocalOp, OneHotModelInfoLocalOp> {

	/**
	 * null constructor.
	 */
	public OneHotTrainLocalOp() {
		super(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public OneHotTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public OneHotTrainLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		final String[] selectedColNames = getSelectedCols();
		final String[] selectedColSqlType = new String[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			selectedColSqlType[i] = FlinkTypeConverter.getTypeString(
				TableUtil.findColTypeWithAssertAndHint(in.getSchema(), selectedColNames[i]));
		}
		int[] thresholdArray;

		if (!getParams().contains(OneHotTrainParams.DISCRETE_THRESHOLDS_ARRAY)) {
			thresholdArray = new int[selectedColNames.length];
			Arrays.fill(thresholdArray, getDiscreteThresholds());
		} else {
			thresholdArray = Arrays.stream(getDiscreteThresholdsArray()).mapToInt(Integer::intValue).toArray();
		}

		boolean enableElse = isEnableElse(thresholdArray);

		List <Row> inputRows = in.select(selectedColNames).getOutputTable().getRows();
		List <Tuple3 <Integer, String, Long>> indexedToken = indexedToken(inputRows, true, thresholdArray);

		RowCollector rowCollector = new RowCollector();
		Params meta = new Params()
			.set(HasSelectedCols.SELECTED_COLS, selectedColNames)
			.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColSqlType)
			.set(HasEnableElse.ENABLE_ELSE, enableElse);
		new OneHotModelDataConverter().save(Tuple2.of(meta, indexedToken), rowCollector);

		long[] maxIndexes = new long[selectedColNames.length];
		for (Tuple3 <Integer, String, Long> t3 : indexedToken) {
			maxIndexes[t3.f0] = Math.max(maxIndexes[t3.f0], t3.f2);
		}
		ArrayList <Row> distinctNumber = new ArrayList <>();
		for (int i = 0; i < selectedColNames.length; i++) {
			distinctNumber.add(Row.of(selectedColNames[i], maxIndexes[i] + 1));
		}

		this.setOutputTable(new MTable(rowCollector.getRows(), new OneHotModelDataConverter().getModelSchema()));
		this.setSideOutputTables(new MTable[] {
			new MTable(distinctNumber, new String[] {"selectedCol", "distinctTokenNumber"},
				new TypeInformation[] {Types.STRING, Types.LONG})
		});
		return this;
	}

	@Override
	public OneHotModelInfoLocalOp getModelInfoLocalOp() {
		return new OneHotModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

	/**
	 * If the thresholdArray is set and greater than 0, enableElse is true.
	 *
	 * @param thresholdArray thresholds for each column.
	 * @return enableElse.
	 */
	private static boolean isEnableElse(int[] thresholdArray) {
		for (int threshold : thresholdArray) {
			if (threshold > 0) {
				return true;
			}
		}
		return false;
	}

	private static List <Tuple3 <Integer, String, Long>> indexedToken(List <Row> data, final boolean ignoreNull,
																	  int[] thresholdArray) {
		int nCol = data.get(0).getArity();
		ArrayList <Tuple3 <Integer, String, Long>> out = new ArrayList <>();
		for (int columnIndex = 0; columnIndex < nCol; columnIndex++) {
			long nullNumber = 0;
			HashMap <String, Long> tokenNumber = new HashMap <>();
			for (Row row : data) {
				Object o = row.getField(columnIndex);
				if (o == null) {
					if (ignoreNull) {
						continue;
					} else {
						nullNumber++;
					}
				} else {
					String value = String.valueOf(o);
					long thisNumber = tokenNumber.getOrDefault(value, 0L);
					tokenNumber.put(value, 1 + thisNumber);
				}
			}
			long index = 0L;
			if (nullNumber != 0 && nullNumber >= thresholdArray[columnIndex]) {
				out.add(Tuple3.of(columnIndex, null, index));
				index++;
			}
			for (Entry <String, Long> entry : tokenNumber.entrySet()) {
				if (entry.getValue() >= thresholdArray[columnIndex]) {
					out.add(Tuple3.of(columnIndex, entry.getKey(), index));
					index++;
				}
			}
		}
		return out;
	}

}
