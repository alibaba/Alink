from .bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion

__all__ = ['FmRegressorModelTrainInfo', 'FmClassifierModelTrainInfo', 'LinearModelTrainInfo',
           'Word2VecTrainInfo', 'Report']


class FmRegressorModelTrainInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.fm.FmRegressorModelTrainInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getMeta(self):
        return self.getMeta()

    def getConvInfo(self):
        return self.getConvInfo()


class FmClassifierModelTrainInfo(FmRegressorModelTrainInfo):
    _j_cls_name = 'com.alibaba.alink.operator.common.fm.FmClassifierModelTrainInfo'

    def __init__(self, j_obj):
        super(FmClassifierModelTrainInfo, self).__init__(j_obj)


class LinearModelTrainInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.linear.LinearModelTrainInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getImportance(self):
        return self.getImportance()

    def getWeight(self):
        return self.getWeight()

    def getColNames(self):
        return self.getColNames()

    def getMeta(self):
        return self.getMeta()

    def getConvInfo(self):
        return self.getConvInfo()


class Word2VecTrainInfo(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.operator.common.nlp.Word2VecTrainInfo'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getNumIter(self):
        return self.getNumIter()

    def getLoss(self):
        return self.getLoss()

    def getNumVocab(self):
        return self.getNumVocab()


class Report(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.pipeline.tuning.Report'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def toPrettyJson(self):
        return self.toPrettyJson()
