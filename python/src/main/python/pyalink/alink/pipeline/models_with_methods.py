from .common import GeneralizedLinearRegressionModel as _GeneralizedLinearRegressionModel
from .common import Word2VecModel as _Word2VecModel
from ..batch.base import BatchOperatorWrapper, BatchOperator

__all__ = ['Word2VecModel', 'GeneralizedLinearRegressionModel']


class Word2VecModel(_Word2VecModel):
    def getVectors(self) -> BatchOperator:
        """
        Get vectors as a :py:class:`BatchOperator`.

        :return: vectors.
        """

        return BatchOperatorWrapper(self.get_j_obj().getVectors())


class GeneralizedLinearRegressionModel(_GeneralizedLinearRegressionModel):
    def evaluate(self, data: BatchOperator):
        """
        Link the model in `self` and `data` to a :py:class:`GlmEvaluationBatchOp`.

        :param data: data to be evaluated.
        :return: a :py:class:`GlmEvaluationBatchOp`.
        """
        return BatchOperatorWrapper(self.get_j_obj().evaluate(data.get_j_obj()))
