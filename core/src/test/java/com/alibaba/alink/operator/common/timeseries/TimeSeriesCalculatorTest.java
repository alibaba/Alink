package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.BoxPlotDetectorCalc;
import com.alibaba.alink.operator.common.outlier.tsa.tsacalculator.KSigmaDetectorCalc;
import com.alibaba.alink.params.outlier.HasBoxPlotK;
import com.alibaba.alink.params.outlier.HasKSigmaK;
import com.alibaba.alink.params.outlier.tsa.HasBoxPlotRoundMode;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class TimeSeriesCalculatorTest extends AlinkTestBase {

	Params params = new Params();

	@Test
	public void testKSigma() {
		DenseVector dv = VectorUtil.parseDense("1, 2, 14, 2");
		params.set(HasKSigmaK.K, 2.);
		KSigmaDetectorCalc kSigmaCalc = new KSigmaDetectorCalc(params);
		kSigmaCalc.detect(dv.getData());
		KSigmaDetectorCalc.calcKSigma(dv.getData(), 2., true);
	}

	@Test
	public void testBoxPlot() {
		DenseVector dv = VectorUtil.parseDense("1, 2, 1, 2, 14");
		params.set(HasBoxPlotK.K, 2.);
		params.set(HasBoxPlotRoundMode.ROUND_MODE, HasBoxPlotRoundMode.RoundMode.CEIL);
		BoxPlotDetectorCalc boxPlotMapper = new BoxPlotDetectorCalc(params);
		boxPlotMapper.detect(dv.getData());
		BoxPlotDetectorCalc.calcBoxPlot(dv.getData(), HasBoxPlotRoundMode.RoundMode.CEIL, 2., true);
	}
}