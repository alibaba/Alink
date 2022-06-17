from .bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion
from ..utils.packages import in_ipython

__all__ = ['BinaryClassMetrics', 'MultiClassMetrics', 'ClusterMetrics', 'RegressionMetrics', 'RankingMetrics',
           'MultiLabelMetrics', 'TimeSeriesMetrics']


class BaseMetrics(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.BaseMetrics'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def clone(self):
        return self.clone()

    def serialize(self):
        return self.serialize()


class BaseSimpleClassifierMetrics(BaseMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics'

    def __init__(self, j_obj):
        super(BaseSimpleClassifierMetrics, self).__init__(j_obj)

    def getConfusionMatrix(self):
        return self.getConfusionMatrix()

    def getLabelArray(self):
        return self.getLabelArray()

    def getLogLoss(self):
        return self.getLogLoss()

    def getTotalSamples(self):
        return self.getTotalSamples()

    def getActualLabelFrequency(self):
        return self.getActualLabelFrequency()

    def getActualLabelProportion(self):
        return self.getActualLabelProportion()

    def getAccuracy(self):
        return self.getAccuracy()

    def getMacroAccuracy(self):
        return self.getMacroAccuracy()

    def getMicroAccuracy(self):
        return self.getMicroAccuracy()

    def getWeightedAccuracy(self):
        return self.getWeightedAccuracy()

    def getKappa(self):
        return self.getKappa()

    def getMacroKappa(self):
        return self.getMacroKappa()

    def getMicroKappa(self):
        return self.getMicroKappa()

    def getWeightedKappa(self):
        return self.getWeightedKappa()

    def getMacroPrecision(self):
        return self.getMacroPrecision()

    def getMicroPrecision(self):
        return self.getMicroPrecision()

    def getWeightedPrecision(self):
        return self.getWeightedPrecision()

    def getMacroRecall(self):
        return self.getMacroRecall()

    def getMicroRecall(self):
        return self.getMicroRecall()

    def getWeightedRecall(self):
        return self.getWeightedRecall()

    def getMacroF1(self):
        return self.getMacroF1()

    def getMicroF1(self):
        return self.getMicroF1()

    def getWeightedF1(self):
        return self.getWeightedF1()

    def getMacroSensitivity(self):
        return self.getMacroSensitivity()

    def getMicroSensitivity(self):
        return self.getMicroSensitivity()

    def getWeightedSensitivity(self):
        return self.getWeightedSensitivity()

    def getMacroSpecificity(self):
        return self.getMacroSpecificity()

    def getMicroSpecificity(self):
        return self.getMicroSpecificity()

    def getWeightedSpecificity(self):
        return self.getWeightedSpecificity()

    def getMacroTruePositiveRate(self):
        return self.getMacroTruePositiveRate()

    def getMicroTruePositiveRate(self):
        return self.getMicroTruePositiveRate()

    def getWeightedTruePositiveRate(self):
        return self.getWeightedTruePositiveRate()

    def getMacroTrueNegativeRate(self):
        return self.getMacroTrueNegativeRate()

    def getMicroTrueNegativeRate(self):
        return self.getMicroTrueNegativeRate()

    def getWeightedTrueNegativeRate(self):
        return self.getWeightedTrueNegativeRate()

    def getMacroFalsePositiveRate(self):
        return self.getMacroFalsePositiveRate()

    def getMicroFalsePositiveRate(self):
        return self.getMicroFalsePositiveRate()

    def getWeightedFalsePositiveRate(self):
        return self.getWeightedFalsePositiveRate()

    def getMacroFalseNegativeRate(self):
        return self.getMacroFalseNegativeRate()

    def getMicroFalseNegativeRate(self):
        return self.getMicroFalseNegativeRate()

    def getWeightedFalseNegativeRate(self):
        return self.getWeightedFalseNegativeRate()


class BinaryClassMetrics(BaseSimpleClassifierMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics'

    def __init__(self, j_obj):
        super(BinaryClassMetrics, self).__init__(j_obj)

    def getRocCurve(self):
        return self.getRocCurve()

    def getLorenzeCurve(self):
        return self.getLorenzeCurve()

    def getPrecision(self):
        return self.getPrecision()

    def getRecall(self):
        return self.getRecall()

    def getF1(self):
        return self.getF1()

    def getAuc(self):
        return self.getAuc()

    def getGini(self):
        return self.getGini()

    def getKs(self):
        return self.getKs()

    def getPrc(self):
        return self.getPrc()

    def getPrecisionRecallCurve(self):
        return self.getPrecisionRecallCurve()

    def getLiftChart(self):
        return self.getLiftChart()

    def getThresholdArray(self):
        return self.getThresholdArray()

    def getPrecisionByThreshold(self):
        return self.getPrecisionByThreshold()

    def getSpecificityByThreshold(self):
        return self.getSpecificityByThreshold()

    def getSensitivityByThreshold(self):
        return self.getSensitivityByThreshold()

    def getRecallByThreshold(self):
        return self.getRecallByThreshold()

    def getF1ByThreshold(self):
        return self.getF1ByThreshold()

    def getAccuracyByThreshold(self):
        return self.getAccuracyByThreshold()

    def getKappaByThreshold(self):
        return self.getKappaByThreshold()

    def saveRocCurveAsImage(self, path, isOverwrite):
        self.saveRocCurveAsImage(path, isOverwrite)
        if in_ipython():
            from IPython import display
            # noinspection PyTypeChecker
            display.display(display.Image(path))

    def saveKSAsImage(self, path, isOverwrite):
        self.saveKSAsImage(path, isOverwrite)
        if in_ipython():
            from IPython import display
            # noinspection PyTypeChecker
            display.display(display.Image(path))

    def saveLiftChartAsImage(self, path, isOverwrite):
        self.saveLiftChartAsImage(path, isOverwrite)
        if in_ipython():
            from IPython import display
            # noinspection PyTypeChecker
            display.display(display.Image(path))

    def savePrecisionRecallCurveAsImage(self, path, isOverwrite):
        self.savePrecisionRecallCurveAsImage(path, isOverwrite)
        if in_ipython():
            from IPython import display
            # noinspection PyTypeChecker
            display.display(display.Image(path))

    def saveLorenzCurveAsImage(self, path, isOverwrite):
        self.saveLorenzCurveAsImage(path, isOverwrite)
        if in_ipython():
            from IPython import display
            # noinspection PyTypeChecker
            display.display(display.Image(path))


class MultiClassMetrics(BaseSimpleClassifierMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.MultiClassMetrics'

    def __init__(self, j_obj):
        super(MultiClassMetrics, self).__init__(j_obj)

    def getPrecision(self, label):
        return self.getPrecision(label)

    def getRecall(self, label):
        return self.getRecall(label)

    def getF1(self, label):
        return self.getF1(label)

    def getPredictLabelFrequency(self):
        return self.getPredictLabelFrequency()

    def getPredictLabelProportion(self):
        return self.getPredictLabelProportion()

    def getTruePositiveRate(self, label):
        return self.getTruePositiveRate(label)

    def getTrueNegativeRate(self, label):
        return self.getTrueNegativeRate(label)

    def getFalsePositiveRate(self, label):
        return self.getFalsePositiveRate(label)

    def getFalseNegativeRate(self, label):
        return self.getFalseNegativeRate(label)

    def getSpecificity(self, label):
        return self.getSpecificity(label)

    def getSensitivity(self, label):
        return self.getSensitivity(label)

    def getAccuracy(self, label=None):
        if label is None:
            return self.getAccuracy()
        else:
            return self.getAccuracy(label)

    def getKappa(self, label=None):
        if label is None:
            return self.getKappa()
        else:
            return self.getKappa(label)


class ClusterMetrics(BaseMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.ClusterMetrics'

    def __init__(self, j_obj):
        super(ClusterMetrics, self).__init__(j_obj)

    def getVrc(self):
        return self.getVrc()

    def getCp(self):
        return self.getCp()

    def getSp(self):
        return self.getSp()

    def getDb(self):
        return self.getDb()

    def getK(self):
        return self.getK()

    def getCount(self):
        return self.getCount()

    def getSsw(self):
        return self.getSsw()

    def getSsb(self):
        return self.getSsb()

    def getClusterArray(self):
        return self.getClusterArray()

    def getCountArray(self):
        return self.getCountArray()

    def getNmi(self):
        return self.getNmi()

    def getPurity(self):
        return self.getPurity()

    def getRi(self):
        return self.getRi()

    def getAri(self):
        return self.getAri()

    def getSilhouetteCoefficient(self):
        return self.getSilhouetteCoefficient()

    def getConfusionMatrix(self):
        return self.getConfusionMatrix()


class RegressionMetrics(BaseMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.RegressionMetrics'

    def __init__(self, j_obj):
        super(RegressionMetrics, self).__init__(j_obj)

    def getCount(self):
        return self.getCount()

    def getMse(self):
        return self.getMse()

    def getMae(self):
        return self.getMae()

    def getRmse(self):
        return self.getRmse()

    def getExplainedVariance(self):
        return self.getExplainedVariance()

    def getSse(self):
        return self.getSse()

    def getSst(self):
        return self.getSst()

    def getSsr(self):
        return self.getSsr()

    def getR2(self):
        return self.getR2()

    def getR(self):
        return self.getR()

    def getSae(self):
        return self.getSae()

    def getMape(self):
        return self.getMape()

    def getYMean(self):
        return self.getYMean()

    def getPredictionMean(self):
        return self.getPredictionMean()


class BaseSimpleMultiLabelMetrics(BaseMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.BaseSimpleMultiLabelMetrics'

    def __init__(self, j_obj):
        super(BaseSimpleMultiLabelMetrics, self).__init__(j_obj)

    def getPrecision(self):
        return self.getPrecision()

    def getRecall(self):
        return self.getRecall()

    def getF1(self):
        return self.getF1()

    def getAccuracy(self):
        return self.getAccuracy()

    def getHammingLoss(self):
        return self.getHammingLoss()

    def getMicroPrecision(self):
        return self.getMicroPrecision()

    def getMicroRecall(self):
        return self.getMicroRecall()

    def getMicroF1(self):
        return self.getMicroF1()

    def getSubsetAccuracy(self):
        return self.getSubsetAccuracy()


class RankingMetrics(BaseSimpleMultiLabelMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.RankingMetrics'

    def __init__(self, j_obj):
        super(RankingMetrics, self).__init__(j_obj)

    def getNdcg(self, k):
        return self.getNdcg(k)

    def getPrecisionAtK(self, k):
        return self.getPrecisionAtK(k)

    def getHitRate(self):
        return self.getHitRate()

    def getArHr(self):
        return self.getArHr()

    def getRecallAtK(self, k):
        return self.getRecallAtK(k)

    def getMap(self):
        return self.getMap()


class MultiLabelMetrics(BaseSimpleMultiLabelMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.MultiLabelMetrics'

    def __init__(self, j_obj):
        super(MultiLabelMetrics, self).__init__(j_obj)


class TimeSeriesMetrics(BaseMetrics):
    _j_cls_name = 'com.alibaba.alink.operator.common.evaluation.TimeSeriesMetrics'

    def __init__(self, j_obj):
        super(TimeSeriesMetrics, self).__init__(j_obj)

    def getMse(self):
        return self.getMse()

    def getMae(self):
        return self.getMae()

    def getRmse(self):
        return self.getRmse()

    def getExplainedVariance(self):
        return self.getExplainedVariance()

    def getSse(self):
        return self.getSse()

    def getSst(self):
        return self.getSst()

    def getSsr(self):
        return self.getSsr()

    def getSae(self):
        return self.getSae()

    def getMape(self):
        return self.getMape()

    def getSmape(self):
        return self.getSmape()

    def getND(self):
        return self.getND()

    def getCount(self):
        return self.getCount()

    def getYMean(self):
        return self.getYMean()

    def getPredictionMean(self):
        return self.getPredictionMean()
