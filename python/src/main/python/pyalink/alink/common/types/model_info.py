from .bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion
from ..utils.packages import in_ipython

__all__ = ['ClusteringModelInfo',
           'KMeansModelInfo',
           'BisectingKMeansModelInfo',
           'GmmModelInfo',
           'OneHotModelInfo',
           'QuantileDiscretizerModelInfo',
           'NaiveBayesTextModelInfo',
           'LdaModelInfo',
           'MaxAbsScalarModelInfo',
           'MinMaxScalerModelInfo',
           'StandardScalerModelInfo',
           'VectorMaxAbsScalarModelInfo',
           'VectorMinMaxScalerModelInfo',
           'VectorStandardScalerModelInfo',
           'EqualWidthDiscretizerModelInfo',
           'ChisqSelectorModelInfo',
           'PcaModelData',
           'FmRegressorModelInfo',
           'FmClassifierModelInfo',
           'LinearRegressorModelInfo',
           'LinearClassifierModelInfo',
           'SoftmaxModelInfo',
           'TreeModelInfo',
           'MultiTreeModelInfo',
           'DecisionTreeModelInfo',
           'RandomForestModelInfo',
           'GbdtModelInfo',
           'NaiveBayesModelInfo',
           'ImputerModelInfo',
           'GlmModelInfo']


class ClusteringModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.clustering.ClusteringModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getClusterCenter(self, clusterId):
        return self.getClusterCenter(clusterId)

    def getClusterNumber(self):
        return self.getClusterNumber()


class KMeansModelInfo(ClusteringModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.batch.clustering.KMeansModelInfoBatchOp$KMeansModelInfo'

    def __init__(self, j_obj):
        super(KMeansModelInfo, self).__init__(j_obj)

    def getClusterWeight(self, clusterId):
        return self.getClusterWeight(clusterId)


class BisectingKMeansModelInfo(ClusteringModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.batch.clustering.BisectingKMeansModelInfoBatchOp$BisectingKMeansModelInfo'

    def __init__(self, j_obj):
        super(BisectingKMeansModelInfo, self).__init__(j_obj)


class GmmModelInfo(ClusteringModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.batch.clustering.GmmModelInfoBatchOp$GmmModelInfo'

    def __init__(self, j_obj):
        super(GmmModelInfo, self).__init__(j_obj)

    def getClusterCovarianceMatrix(self, clusterId):
        return self.getClusterCovarianceMatrix(clusterId)


class OneHotModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.feature.OneHotModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getSelectedColsInModel(self):
        return self.getSelectedColsInModel()

    def getDistinctTokenNumber(self, columnName):
        return self.getDistinctTokenNumber(columnName)

    def getTokens(self, columnName):
        return self.getTokens(columnName)


class QuantileDiscretizerModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getSelectedColsInModel(self):
        return self.getSelectedColsInModel()

    def getCutsArray(self, columnName):
        return self.getCutsArray(columnName)

    _unsupported_j_methods = ['mapToString']


class NaiveBayesTextModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.batch.classification.NaiveBayesTextModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getVectorColName(self):
        return self.getVectorColName()

    def getModelType(self):
        return self.getModelType()

    def getLabelList(self):
        return self.getLabelList()

    def getPriorProbability(self):
        return self.getPriorProbability()

    def getFeatureProbability(self):
        return self.getFeatureProbability()

    _unsupported_j_methods = ['generateLabelProportionTable']


class LdaModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.batch.clustering.LdaModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getLogPerplexity(self):
        return self.getLogPerplexity()

    def getLogLikelihood(self):
        return self.getLogLikelihood()

    def getTopicNum(self):
        return self.getTopicNum()

    def getVocabularySize(self):
        return self.getVocabularySize()


class MaxAbsScalarModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.dataproc.MaxAbsScalarModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getMaxAbs(self):
        return self.getMaxAbs()


class MinMaxScalerModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getMaxs(self):
        return self.getMaxs()

    def getMins(self):
        return self.getMins()


class StandardScalerModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.dataproc.StandardScalerModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getStdDevs(self):
        return self.getStdDevs()

    def getMeans(self):
        return self.getMeans()

    def isWithMeans(self):
        return self.isWithMeans()

    def isWithStdDevs(self):
        return self.isWithStdDevs()


class VectorMaxAbsScalarModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalarModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getMaxAbs(self):
        return self.getMaxAbs()


class VectorMinMaxScalerModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getMaxs(self):
        return self.getMaxs()

    def getMins(self):
        return self.getMins()


class VectorStandardScalerModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getStdDevs(self):
        return self.getStdDevs()

    def getMeans(self):
        return self.getMeans()

    def isWithMeans(self):
        return self.isWithMeans()

    def isWithStdDevs(self):
        return self.isWithStdDevs()


class EqualWidthDiscretizerModelInfo(QuantileDiscretizerModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerModelInfoBatchOp$EqualWidthDiscretizerModelInfo'

    def __init__(self, j_obj):
        super(EqualWidthDiscretizerModelInfo, self).__init__(j_obj)

    def get_j_obj(self):
        return self._j_obj

    _unsupported_j_methods = ['mapToString']


class ChisqSelectorModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.feature.ChisqSelectorModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def chisq(self, arg0):
        return self.chisq(arg0)

    def pValue(self, arg0):
        return self.pValue(arg0)

    def getSelectorType(self):
        return self.getSelectorType()

    def getNumTopFeatures(self):
        return self.getNumTopFeatures()

    def getPercentile(self):
        return self.getPercentile()

    def getFpr(self):
        return self.getFpr()

    def getFdr(self):
        return self.getFdr()

    def getFwe(self):
        return self.getFwe()

    def getSelectorNum(self):
        return self.getSelectorNum()

    def getColNames(self):
        return self.getColNames()

    def getSiftOutColNames(self):
        return self.getSiftOutColNames()


class PcaModelData(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.feature.pca.PcaModelData'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getCols(self):
        return self.getCols()

    def getEigenValues(self):
        return self.getEigenValues()

    def getEigenVectors(self):
        return self.getEigenVectors()

    def getProportions(self):
        return self.getProportions()

    def getCumulatives(self):
        return self.getCumulatives()


class FmRegressorModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.fm.FmRegressorModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def hasIntercept(self):
        return self.hasIntercept()

    def hasLinearItem(self):
        return self.hasLinearItem()

    def getNumFactor(self):
        return self.getNumFactor()

    def getTask(self):
        return self.getTask()

    def getNumFeature(self):
        return self.getNumFeature()

    def getFactors(self):
        return self.getFactors()

    def getFeatureColNames(self):
        return self.getFeatureColNames()


class FmClassifierModelInfo(FmRegressorModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.common.fm.FmClassifierModelInfo'

    def __init__(self, j_obj):
        super(FmClassifierModelInfo, self).__init__(j_obj)

    def getLabelValues(self):
        return self.getLabelValues()


class LinearRegressorModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def hasInterceptItem(self):
        return self.hasInterceptItem()

    def getFeatureNames(self):
        return self.getFeatureNames()

    def getVectorColName(self):
        return self.getVectorColName()

    def getWeight(self):
        return self.getWeight()

    def getVectorSize(self):
        return self.getVectorSize()

    def getModelName(self):
        return self.getModelName()


class LinearClassifierModelInfo(LinearRegressorModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.common.linear.LinearClassifierModelInfo'

    def __init__(self, j_obj):
        super(LinearClassifierModelInfo, self).__init__(j_obj)

    def getLabelValues(self):
        return self.getLabelValues()


class SoftmaxModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.linear.SoftmaxModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def hasInterceptItem(self):
        return self.hasInterceptItem()

    def getFeatureNames(self):
        return self.getFeatureNames()

    def getVectorColName(self):
        return self.getVectorColName()

    def getVectorSize(self):
        return self.getVectorSize()

    def getModelName(self):
        return self.getModelName()

    def getWeights(self):
        return self.getWeights()

    def getLabelValues(self):
        return self.getLabelValues()


class TreeModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.tree.TreeModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getFeatureImportance(self):
        return self.getFeatureImportance()

    def getNumTrees(self):
        return self.getNumTrees()

    def getFeatures(self):
        return self.getFeatures()

    def getCategoricalFeatures(self):
        return self.getCategoricalFeatures()

    def getCategoricalValues(self, categoricalCol):
        return self.getCategoricalValues(categoricalCol)

    def getLabels(self):
        return self.getLabels()


class MultiTreeModelInfo(TreeModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.common.tree.TreeModelInfo$MultiTreeModelInfo'

    def __init__(self, j_obj):
        super(MultiTreeModelInfo, self).__init__(j_obj)

    def getCaseWhenRule(self, treeId):
        return self.getCaseWhenRule(treeId)

    def saveTreeAsImage(self, path, treeId, isOverwrite):
        self.saveTreeAsImage(path, treeId, isOverwrite)
        if in_ipython():
            from IPython import display
            # noinspection PyTypeChecker
            display.display(display.Image(path))


class DecisionTreeModelInfo(TreeModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.common.tree.TreeModelInfo$DecisionTreeModelInfo'

    def __init__(self, j_obj):
        super(DecisionTreeModelInfo, self).__init__(j_obj)

    def getCaseWhenRule(self):
        return self.getCaseWhenRule()

    def saveTreeAsImage(self, path, isOverwrite):
        self.saveTreeAsImage(path, isOverwrite)
        if in_ipython():
            from IPython import display
            # noinspection PyTypeChecker
            display.display(display.Image(path))


class RandomForestModelInfo(MultiTreeModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.common.tree.TreeModelInfo$RandomForestModelInfo'

    def __init__(self, j_obj):
        super(RandomForestModelInfo, self).__init__(j_obj)


class GbdtModelInfo(MultiTreeModelInfo):
    _j_cls_name = 'com.alibaba.alink.operator.common.tree.TreeModelInfo$GbdtModelInfo'

    def __init__(self, j_obj):
        super(GbdtModelInfo, self).__init__(j_obj)


class NaiveBayesModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.batch.classification.NaiveBayesModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getFeatureNames(self):
        return self.getFeatureNames()

    def getCategoryFeatureInfo(self):
        return self.getCategoryFeatureInfo()

    def getGaussFeatureInfo(self):
        return self.getGaussFeatureInfo()

    def getLabelList(self):
        return self.getLabelList()

    def getLabelProportion(self):
        return self.getLabelProportion()

    def getCategoryInfo(self):
        return self.getCategoryInfo()


class ImputerModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.dataproc.ImputerModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def fillValue(self):
        return self.fillValue()

    def getFillValues(self):
        return self.getFillValues()

    def getStrategy(self):
        return self.getStrategy()


class GlmModelInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.regression.glm.GlmModelInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getIntercept(self):
        return self.getIntercept()

    def getFeatureColNames(self):
        return self.getFeatureColNames()

    def getLabelColName(self):
        return self.getLabelColName()

    def getVariancePower(self):
        return self.getVariancePower()

    def getLink(self):
        return self.getLink()

    def getLinkPower(self):
        return self.getLinkPower()

    def getCoefficients(self):
        return self.getCoefficients()

    def isFitIntercept(self):
        return self.isFitIntercept()

    def getDegreeOfFreedom(self):
        return self.getDegreeOfFreedom()

    def getResidualDegreeOfFreeDom(self):
        return self.getResidualDegreeOfFreeDom()

    def getResidualDegreeOfFreedomNull(self):
        return self.getResidualDegreeOfFreedomNull()

    def getAic(self):
        return self.getAic()

    def getDispersion(self):
        return self.getDispersion()

    def getDeviance(self):
        return self.getDeviance()

    def getNullDeviance(self):
        return self.getNullDeviance()

    def getTValues(self):
        return self.getTValues()

    def getPValues(self):
        return self.getPValues()

    def getStdErrors(self):
        return self.getStdErrors()

    def getFamily(self):
        return self.getFamily()
