package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.BoxPlotDetectorCalc;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.ConvolutionDecomposerCalc;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.DecomposeOutlierDetectorCalc;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.KSigmaDetectorCalc;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.SHESDDetectorCalc;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.STLDecomposerCalc;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.TimeSeriesDecomposerCalc;
import com.alibaba.alink.params.shared.colname.HasTimeColDefaultAsNull;

import java.util.Map;

public class TimeSeriesDecomposeDetector extends OutlierDetector {

	private final String selectedCol;
	private final String timeCol;

	public TimeSeriesDecomposeDetector(TableSchema dataSchema,
									   Params params) {
		super(dataSchema, params);
		this.selectedCol = params.contains(TimeSeriesDecomposeParams.SELECTED_COL) ? params.get(
			TimeSeriesDecomposeParams.SELECTED_COL)
			: dataSchema.getFieldNames()[0];
		this.timeCol = params.get(HasTimeColDefaultAsNull.TIME_COL);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		if (null != timeCol) {
			series.orderBy(timeCol);
		}
		int colIndex = TableUtil.findColIndex(series.getSchema(), selectedCol);
		int n = series.getNumRow();
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = ((Number) series.getEntry(i, colIndex)).doubleValue();
		}

		TimeSeriesDecomposerCalc timeSeriesDecomposer = null;
		switch (params.get(TimeSeriesDecomposeParams.DECOMPOSE_METHOD)) {
			case CONVOLUTION:
				timeSeriesDecomposer = new ConvolutionDecomposerCalc(params);
				break;
			default:
				timeSeriesDecomposer = new STLDecomposerCalc(params);
		}

		DenseVector[] decomposedData = null;
		String errorDetail = "";
		try {
			decomposedData = timeSeriesDecomposer.decompose(data);
		} catch (Exception ex) {
			//ex.printStackTrace();
			errorDetail = ex.getMessage();
		}

		int[] outlierIndexes = null;
		if (decomposedData != null) {
			DecomposeOutlierDetectorCalc decomposeOutlierDetector = null;
			switch (params.get(TimeSeriesDecomposeParams.DETECT_METHOD)) {
				case SHESD:
					decomposeOutlierDetector = new SHESDDetectorCalc(params);
					break;
				case BoxPlot:
					decomposeOutlierDetector = new BoxPlotDetectorCalc(params);
					break;
				default:
					decomposeOutlierDetector = new KSigmaDetectorCalc(params);
			}
			outlierIndexes = decomposeOutlierDetector.detect(decomposedData[2].getData());
		}

		Tuple3 <Boolean, Double, Map <String, String>>[] tuple3s = new Tuple3[n];
		for (int i = 0; i < n; i++) {
			tuple3s[i] = Tuple3.of(false, null, null);
		}
		if (outlierIndexes != null) {
			for (int idx : outlierIndexes) {
				tuple3s[idx].f0 = true;
			}
		}

		return tuple3s;
	}
}
