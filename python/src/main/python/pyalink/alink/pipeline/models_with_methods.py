from .base import BatchOperatorWrapper
from .common import GeneralizedLinearRegressionModel as _GeneralizedLinearRegressionModel
from .common import Word2VecModel as _Word2VecModel


class Word2VecModel(_Word2VecModel):
    def getVectors(self):
        return BatchOperatorWrapper(self.get_j_obj().getVectors())


class GeneralizedLinearRegressionModel(_GeneralizedLinearRegressionModel):
    def evaluate(self, data):
        return BatchOperatorWrapper(self.get_j_obj().evaluate(data.get_j_obj()))
