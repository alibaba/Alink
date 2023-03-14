//package com.alibaba.alink.operator.common.timeseries.arimagarch;
//
//import com.alibaba.alink.params.timeseries.HasArimaGarchMethod;
//import com.alibaba.alink.params.timeseries.HasArimaGarchMethod.ArimaGarchMethod;
//import com.alibaba.alink.params.timeseries.HasIcType;
//import com.alibaba.alink.params.timeseries.HasIcType.IcType;
//import org.junit.Test;
//
//import static org.junit.Assert.*;
//
//public class ArimaGarchTest {
//
//	@Test
//	public void testArimaGarch() {
//		double[] vals = new double[] {3922590, 24030070, 43180940, 41085380, 45033620, 64018920, 62424010, 61754130,
//			63327410, 63504950, 163191300, 66835180, 38004530, 70127840, 83796470, 71638020, 84964550, 122612700,
//			91573510, 114713000, 125689500, 126691900, 297069700, 105349400, 90628430, 62466880, 148581700, 129903600,
//			140039500, 230912500, 152089500, 179737200, 181065700, 187005200, 458144500};
//
//		ArimaGarch.autoFit(vals, IcType.AIC, ArimaGarchMethod.CONSIST, 5, 5, true);
//		ArimaGarch.autoFit(vals, IcType.AIC, HasArimaGarchMethod.ArimaGarchMethod.SEPARATE, 5, 5, true);
//		ArimaGarch.autoFit(vals, IcType.AIC, HasArimaGarchMethod.ArimaGarchMethod.SEPARATE, 5, 5, true);
//
//		SeparateEstimate separateEstimate = new SeparateEstimate();
//		separateEstimate.bfgsEstimate(vals, HasIcType.IcType.AIC, 1, 1, false);
//
//	}
//
//}