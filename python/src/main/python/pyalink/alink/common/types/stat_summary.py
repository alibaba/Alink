from .bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion, JavaObjectWrapper

__all__ = ['TableSummary', 'SparseVectorSummary', 'DenseVectorSummary',
           'CorrelationResult', 'ChiSquareTestResult', 'ChiSquareTestResults',
           'FullStats']


class BaseSummary(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.basicstatistic.BaseSummary'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def count(self):
        return self.count()


class TableSummary(BaseSummary):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary'

    def __init__(self, j_obj):
        super(TableSummary, self).__init__(j_obj)

    def min(self, colName):
        return self.min(colName)

    def max(self, colName):
        return self.max(colName)

    def sum(self, colName):
        return self.sum(colName)

    def normL1(self, colName):
        return self.normL1(colName)

    def numMissingValue(self, colName):
        return self.numMissingValue(colName)

    def getColNames(self):
        return self.getColNames()

    def mean(self, colName):
        return self.mean(colName)

    def variance(self, colName):
        return self.variance(colName)

    def standardDeviation(self, colName):
        return self.standardDeviation(colName)

    def normL2(self, colName):
        return self.normL2(colName)

    def centralMoment2(self):
        return self.centralMoment2()

    def centralMoment3(self):
        return self.centralMoment3()

    def numValidValue(self, colName):
        return self.numValidValue(colName)


class BaseVectorSummary(BaseSummary):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary'

    def __init__(self, j_obj):
        super(BaseSummary, self).__init__(j_obj)

    def min(self, idx=None):
        if idx is None:
            return self.min()
        else:
            return self.min(idx)

    def max(self, idx=None):
        if idx is None:
            return self.max()
        else:
            return self.max(idx)

    def sum(self, idx=None):
        if idx is None:
            return self.sum()
        else:
            return self.sum(idx)

    def vectorSize(self):
        return self.vectorSize()

    def normL1(self, idx=None):
        if idx is None:
            return self.normL1()
        else:
            return self.normL1(idx)

    def mean(self, idx=None):
        if idx is None:
            return self.mean()
        else:
            return self.mean(idx)

    def variance(self, idx=None):
        if idx is None:
            return self.variance()
        else:
            return self.variance(idx)

    def standardDeviation(self, idx=None):
        if idx is None:
            return self.standardDeviation()
        else:
            return self.standardDeviation(idx)

    def normL2(self, idx=None):
        if idx is None:
            return self.normL2()
        else:
            return self.normL2(idx)


class SparseVectorSummary(BaseVectorSummary):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.basicstatistic.SparseVectorSummary'

    def __init__(self, j_obj):
        super(BaseVectorSummary, self).__init__(j_obj)

    def numNonZero(self, idx=None):
        if idx is None:
            return self.numNonZero()
        else:
            return self.numNonZero(idx)


class DenseVectorSummary(BaseVectorSummary):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.basicstatistic.DenseVectorSummary'

    def __init__(self, j_obj):
        super(BaseVectorSummary, self).__init__(j_obj)


class CorrelationResult(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getCorrelation(self):
        return self.getCorrelation()

    def getCorrelationMatrix(self):
        return self.getCorrelationMatrix()

    def getColNames(self):
        return self.getColNames()


class ChiSquareTestResult(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.ChiSquareTestResult'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getValue(self):
        return self.getValue()

    def getDf(self):
        return self.getDf()

    def getP(self):
        return self.getP()

    def getColName(self):
        return self.getColName()

    def setColName(self, colName):
        return self.setColName(colName)


class ChiSquareTestResults(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.ChiSquareTestResults'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getSelectedCols(self):
        return self.getSelectedCols()

    def getResults(self):
        return self.getResults()

    def getResult(self, colName: str):
        return self.getResult(colName)


class FullStats(JavaObjectWrapper):
    _j_cls_name = 'com.alibaba.alink.operator.common.statistics.statistics.FullStats'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getDatasetFeatureStatisticsList(self):
        j_proto_value = self.get_j_obj().getDatasetFeatureStatisticsList()
        try:
            # If package `tensorflow_metadata` is installed, return Python version, otherwise return Java version.
            from tensorflow_metadata.proto import statistics_pb2
            buffer = j_proto_value.toByteArray()
            py_proto_value = statistics_pb2.DatasetFeatureStatisticsList()
            py_proto_value.ParseFromString(buffer)
            return py_proto_value
        except ImportError:
            return j_proto_value
        except Exception as e:
            raise e
