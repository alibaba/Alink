import warnings
from abc import ABC
from typing import Optional, Union, List

import deprecation
from py4j.java_gateway import JavaObject

from .local_predictor import LocalPredictable
from .mixins import HasLazyPrintTransformInfo
from ..batch.base import BatchOperator, BatchOperatorWrapper
from ..common.types.bases.model_stream_scan_params import ModelStreamScanParams
from ..common.types.bases.params import Params
from ..common.types.bases.with_params import WithParams
from ..common.types.conversion.type_converters import py_list_to_j_array
from ..common.types.file_system.file_system import FilePath
from ..py4j_util import get_java_gateway, get_java_class
from ..stream.base import StreamOperatorWrapper, StreamOperator

LAZY_PRINT_TRANSFORM_DATA_ENABLED = "lazyPrintTransformDataEnabled"
LAZY_PRINT_TRANSFORM_DATA_TITLE = "lazyPrintTransformDataTitle"
LAZY_PRINT_TRANSFORM_DATA_NUM = "lazyPrintTransformDataNum"
LAZY_PRINT_TRANSFORM_STAT_ENABLED = "lazyPrintTransformStatEnabled"
LAZY_PRINT_TRANSFORM_STAT_TITLE = "lazyPrintTransformStatTitle"

LAZY_PRINT_TRAIN_INFO_ENABLED = "lazyPrintTrainInfoEnabled"
LAZY_PRINT_TRAIN_INFO_TITLE = "lazyPrintTrainInfoTitle"

LAZY_PRINT_MODEL_INFO_ENABLED = "lazyPrintModelInfoEnabled"
LAZY_PRINT_MODEL_INFO_TITLE = "lazyPrintModelInfoTitle"

__all__ = ['Transformer', 'Model', 'PipelineModel', 'Estimator', 'Pipeline', 'TuningEvaluator']


class Transformer(WithParams, HasLazyPrintTransformInfo):
    """
    The base class of all :py:class:`Transformer` s.
    Its instance wraps a Java object of type `Transformer`.
    """

    def __init__(self, j_transformer: Optional[JavaObject] = None, *args, **kwargs):
        """
        Construct a Java object, then set its parameters with a :py:class:`Params` instance.

        The :py:class:`Params` instance is constructed from `args` and `kwargs` using :py:func:`Params.from_args`.

        Following ways of constructors are supported:

        #. if `j_transformer` is not `None`, directly wrap it;
        #. if `j_transformer` is `None`, construct a Java object of class `cls_name` with empty constructor (`cls_name` is obtained from `kwargs` with key `CLS_NAME`);

        `name` and `OP_TYPE` are optionally extracted from `kwargs` with key `name` and `OP_TYPE` respectively.

        :param j_transformer: a Java `Transformer` object.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)
        clsName = kwargs.pop('CLS_NAME', None)
        self.opType = kwargs.pop('OP_TYPE', 'FUNCTION')
        if j_transformer is None:
            self.j_transformer = get_java_gateway().jvm.__getattr__(clsName)()
        else:
            self.j_transformer = j_transformer

    def get_j_obj(self):
        return self.j_transformer

    def transform(self, op: Union[BatchOperator, StreamOperator]) -> Union[BatchOperator, StreamOperator]:
        """
        Apply transformation on operator `op`.

        :param op: the operator for the transformation to be applied.
        :return: the transformation result.
        """

        if isinstance(op, BatchOperator):
            return BatchOperatorWrapper(self.get_j_obj().transform(op.get_j_obj()))
        else:
            return StreamOperatorWrapper(self.get_j_obj().transform(op.get_j_obj()))

    @staticmethod
    def wrap_transformer(j_transformer: JavaObject):
        """
        Wrap a Java instance of `Transformer` to a Python object of corresponding class.

        :param j_transformer: a Java instance of `Transformer`.
        :return: a Python object with corresponding class.
        """
        model_name = j_transformer.getClass().getSimpleName()
        import importlib
        model_cls = getattr(importlib.import_module("pyalink.alink.pipeline"), model_name)
        return model_cls(j_transformer=j_transformer)


class TransformerWrapper(Transformer):
    def __init__(self, j_transformer):
        super(TransformerWrapper, self).__init__(j_transformer=j_transformer)


class Model(Transformer):
    """
    The base class of all :py:class:`Model` s.
    Its instance wraps a Java object of type `Model`.
    """

    def __init__(self, j_model: Optional[JavaObject] = None, *args, **kwargs):
        """
        Construct a Java object, then set its parameters with a :py:class:`Params` instance.

        The :py:class:`Params` instance is constructed from `args` and `kwargs` using :py:func:`Params.from_args`.

        Following ways of constructors are supported:

        #. if `j_model` is not `None`, directly wrap it;
        #. if `j_model` is `None`, construct a Java object of class `cls_name` with empty constructor (`cls_name` is obtained from `kwargs` with key `CLS_NAME`);

        `name` and `OP_TYPE` are optionally extracted from `kwargs` with key `name` and `OP_TYPE` respectively.

        :param j_model: a Java `Model` object.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(Model, self).__init__(j_transformer=j_model, *args, **kwargs)

    def getModelData(self) -> BatchOperator:
        """
        Get the model data.

        :return: model data.
        """
        return BatchOperatorWrapper(self.get_j_obj().getModelData())

    def setModelData(self, model_data: BatchOperator):
        """
        Set the model data.

        :param model_data: model data.
        :return: `self`.
        """
        self.get_j_obj().setModelData(model_data.get_j_obj())
        return self

    @staticmethod
    def wrap_model(j_model: JavaObject):
        """
        Wrap a Java instance of `Model` to a Python object of corresponding class.

        :param j_model: a Java instance of `Model`.
        :return: a Python object with corresponding class.
        """
        model_name = j_model.getClass().getSimpleName()
        import importlib
        model_cls = getattr(importlib.import_module("pyalink.alink.pipeline"), model_name)
        return model_cls(j_model=j_model)


class PipelineModel(Model, ModelStreamScanParams, LocalPredictable):
    """
    A Python equivalent to Java `PipelineModel`.
    """

    def __init__(self, *args, **kwargs):
        """
        Construct a Java object of `PipelineModel` and wrap it.

        Following ways of constructors are supported:

        #. if `args` has only 1 entry and the entry is an instance of `JavaObject`, directly wrap it;
        #. Otherwise, construct a Java object of `PipelineModel` with `args` as stages.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_model = args[0]
        else:
            j_pipeline_model_cls = get_java_class("com.alibaba.alink.pipeline.PipelineModel")
            j_transformer_base_cls = get_java_class("com.alibaba.alink.pipeline.TransformerBase")
            j_transformers = list(map(lambda d: d.get_j_obj(), args))
            j_transformer_arr = py_list_to_j_array(j_transformer_base_cls, len(args), j_transformers)
            j_model = j_pipeline_model_cls(j_transformer_arr)
        super().__init__(j_model, **kwargs)

    def getTransformers(self) -> List[Union[Transformer, Model]]:
        """
        Get all stages in this pipeline model.

        :return: all stages.
        """
        j_op_type_util_cls = get_java_class("com.alibaba.alink.python.utils.OpTypeUtil")
        j_transformers = self.get_j_obj().getTransformers()
        transformers = []
        for j_transformer in j_transformers:
            if j_op_type_util_cls.isModelBase(j_transformer):
                transformer = Model.wrap_model(j_transformer)
            elif j_op_type_util_cls.isTransformerBase(j_transformer):
                transformer = Transformer.wrap_transformer(j_transformer)
            else:
                raise ValueError("Invalid transformer: {}".format(j_transformer))
            transformers.append(transformer)
        return transformers

    def save(self, file_path: Optional[Union[str, FilePath]] = None, overwrite: bool = False, numFiles: int = 1):
        """
        Save the data in the pipline model.

        #. if `file_path` is not set, the data is returned as a :py:class:`BatchOperator`;
        #. if `file_path` is set, the data operator is linked to a :py:class:`AkSinkBatchOp` with parameters `overwrite` and `numFiles`.

        :param file_path: if set, file path to be written.
        :param overwrite: whether of overwrite file path.
        :param numFiles: number of files to be written.
        :return: data operator when `file_path` not set.
        """
        if file_path is None:
            return BatchOperatorWrapper(self.get_j_obj().save())
        elif isinstance(file_path, str):
            self.get_j_obj().save(file_path, overwrite)
        elif isinstance(file_path, FilePath):
            self.get_j_obj().save(file_path.get_j_obj(), overwrite, numFiles)
        else:
            raise ValueError("file_path must be str or FilePath")

    @staticmethod
    def collectLoad(op: BatchOperator) -> 'PipelineModel':
        """
        Trigger the program execution like :py:func:`BatchOperator.execute`, and load data stored in `op`
        to a pipeline model.

        :param op: a batch operator storing the data.
        :return: a pipeline model.
        """
        _j_pipeline_model_cls = get_java_class("com.alibaba.alink.pipeline.PipelineModel")
        j_pipeline_model = _j_pipeline_model_cls.collectLoad(op.get_j_obj())
        PipelineModel._check_lazy_params(j_pipeline_model)
        return PipelineModel(j_pipeline_model)

    @staticmethod
    def load(file_path: Union[str, FilePath]) -> 'PipelineModel':
        """
        Load data stored in `file_path` to a pipeline model.
        The `file_path` must be a valid Ak file or directory, i.e. written by :py:class:`AkSinkBatchOp`.
        No program execution is triggered.

        :param file_path: a valid Ak file or directory storing the data.
        :return: a pipeline model.
        """
        _j_pipeline_model_cls = get_java_class("com.alibaba.alink.pipeline.PipelineModel")
        if isinstance(file_path, (str,)):
            path = file_path
            j_pipeline_model = _j_pipeline_model_cls.load(path)
        elif isinstance(file_path, FilePath):
            operator = file_path
            j_pipeline_model = _j_pipeline_model_cls.load(operator.get_j_obj())
        else:
            raise ValueError("file_path must be str or FilePath")

        PipelineModel._check_lazy_params(j_pipeline_model)
        return PipelineModel(j_pipeline_model)

    @staticmethod
    def _check_lazy_params(pipeline_model):
        for j_transformer in pipeline_model.getTransformers():
            params = Params.fromJson(j_transformer.getParams().toJson())
            transformer = TransformerWrapper(j_transformer)

            if params.get(LAZY_PRINT_TRANSFORM_STAT_ENABLED, False):
                transformer.enableLazyPrintTransformStat(params.get(LAZY_PRINT_TRANSFORM_STAT_TITLE))
            if params.get(LAZY_PRINT_TRANSFORM_DATA_ENABLED, False):
                transformer.enableLazyPrintTransformData(
                    params.get(LAZY_PRINT_TRANSFORM_DATA_NUM),
                    params.get(LAZY_PRINT_TRANSFORM_DATA_TITLE))


class Estimator(WithParams, HasLazyPrintTransformInfo):
    """
    The base class of all :py:class:`Estimator` s.
    Its instance wraps a Java object of type `Estimator`.
    """

    def __init__(self, j_estimator: Optional[JavaObject] = None, *args, **kwargs):
        """
        Construct a Java object, then set its parameters with a :py:class:`Params` instance.

        The :py:class:`Params` instance is constructed from `args` and `kwargs` using :py:func:`Params.from_args`.

        Following ways of constructors are supported:

        #. if `j_estimator` is not `None`, directly wrap it;
        #. if `j_estimator` is `None`, construct a Java object of class `cls_name` with empty constructor (`cls_name` is obtained from `kwargs` with key `CLS_NAME`);

        `name` and `OP_TYPE` are optionally extracted from `kwargs` with key `name` and `OP_TYPE` respectively.

        :param j_estimator: a Java `Model` object.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)
        clsName = kwargs.pop('CLS_NAME', None)
        self.opType = kwargs.pop('OP_TYPE', 'FUNCTION')
        if j_estimator is None:
            self._j_estimator = get_java_gateway().jvm.__getattr__(clsName)()
        else:
            self._j_estimator = j_estimator

    def get_j_obj(self):
        return self._j_estimator

    def fit(self, op: BatchOperator):
        """
        Fit a model on given data `op`.

        :param op: data.
        :return: the fitted model.
        """
        j_model = self.get_j_obj().fit(op.get_j_obj())
        return Model.wrap_model(j_model)

    def fitAndTransform(self, op):
        """
        Fit a model and then apply model on the data `op`.

        :param op: data.
        :return: the transformation result.
        """
        if isinstance(op, BatchOperator):
            return BatchOperatorWrapper(self.get_j_obj().fitAndTransform(op.get_j_obj()))
        else:
            return StreamOperatorWrapper(self.get_j_obj().fitAndTransform(op.get_j_obj()))

    @staticmethod
    def wrap_estimator(j_estimator: JavaObject):
        """
        Wrap a Java instance of `Estimator` to a Python object of corresponding class.

        :param j_estimator: a Java instance of `Estimator`.
        :return: a Python object with corresponding class.
        """

        model_name = j_estimator.getClass().getSimpleName()
        import importlib
        model_cls = getattr(importlib.import_module("pyalink.alink.pipeline"), model_name)
        return model_cls(j_estimator=j_estimator)


class EstimatorWrapper(Estimator):
    def __init__(self, j_estimator):
        super(EstimatorWrapper, self).__init__(j_estimator=j_estimator)


class Pipeline(Estimator):
    """
    A Python equivalent to Java `Pipeline`.
    """

    def __init__(self, *stages: Union[Estimator, Transformer], j_pipeline: Optional[JavaObject] = None):
        """
        Construct a Java object of `Pipeline` and wrap it.

        Following ways of constructors are supported:

        #. if `j_pipeline` is not `None`, directly wrap it;
        #. Otherwise, construct a Java object of `Pipeline` with `stages`.

        :param stages: stages.
        :param j_pipeline: a Java object of `Pipeline`.
        """

        super(Estimator, self).__init__()
        j_pipeline_cls = get_java_gateway().jvm.com.alibaba.alink.pipeline.Pipeline
        j_pipeline_stage_base = get_java_gateway().jvm.com.alibaba.alink.pipeline.PipelineStageBase

        self._stages = list(stages)

        if j_pipeline is None:
            num = len(stages)
            args = get_java_gateway().new_array(j_pipeline_stage_base, num)
            for i, stage in enumerate(stages):
                args[i] = stage.get_j_obj()
            self._j_pipeline = j_pipeline_cls(args)
        else:
            self._j_pipeline = j_pipeline

    def get_j_obj(self):
        return self._j_pipeline

    def add(self, *args: Union[Estimator, Transformer]):
        """
        Append a stage to the end of this pipeline.

        Note: there is a deprecated usage which accepts `(index, stage)` as arguments. `Deprecated since version 1.3.0.`

        :param args: a single stage.
        :return: `self`.
        """
        if len(args) == 1:
            index = self.size()
            stage = args[0]
        else:
            index = args[0]
            stage = args[1]
            warnings.warn("usage of add(index, stage) is deprecated", DeprecationWarning, stacklevel=2)
        self.get_j_obj().add(index, stage.get_j_obj())
        self._stages.insert(index, stage)
        return self

    @deprecation.deprecated("1.3.0")
    def remove(self, index):
        """
        Remove stage by `index`.

        :param index: the index of stage to be removed.
        :return: the removed stage.
        """
        self._j_pipeline.remove(index)
        return self._stages.pop(index)

    def get(self, index):
        """
        Get stage by `index`.

        :param index: the index of stage.
        :return: the stage.
        """
        return self._stages[index]

    def size(self):
        """
        Get the number of stages.

        :return: the number of stages.
        """
        return len(self._stages)

    def fit(self, op: BatchOperator) -> PipelineModel:
        """
        Fit on the data operator `op`.

        :param op: the data operator.
        :return: a pipeline model.
        """
        return PipelineModel(self._j_pipeline.fit(op.get_j_obj()))

    def save(self, file_path: Optional[Union[str, FilePath]] = None, overwrite: bool = False, numFiles: int = 1):
        """
        Save the data in the pipline.

        #. if `file_path` is not set, the data is returned as a :py:class:`BatchOperator`;
        #. if `file_path` is set, the data operator is linked to a :py:class:`AkSinkBatchOp` with parameters `overwrite` and `numFiles`.

        :param file_path: if set, file path to be written.
        :param overwrite: whether of overwrite file path.
        :param numFiles: number of files to be written.
        :return: data operator when `file_path` not set.
        """

        if file_path is None:
            return BatchOperatorWrapper(self.get_j_obj().save())
        elif isinstance(file_path, str):
            self.get_j_obj().save(file_path)
        elif isinstance(file_path, FilePath):
            self.get_j_obj().save(file_path.get_j_obj(), overwrite, numFiles)
        else:
            raise ValueError("file_path must be str or FilePath")

    @staticmethod
    def collectLoad(op: BatchOperator) -> 'Pipeline':
        """
        Trigger the program execution like :py:func:`BatchOperator.execute`, and load data stored in `op`
        to a pipeline.

        :param op: a batch operator storing the data.
        :return: a pipeline.
        """

        _j_pipeline_cls = get_java_class("com.alibaba.alink.pipeline.Pipeline")
        j_pipeline = _j_pipeline_cls.collectLoad(op.get_j_obj())
        stages = Pipeline._check_lazy_params(j_pipeline)
        return Pipeline(*stages, j_pipeline=j_pipeline)

    @staticmethod
    def load(file_path: Union[str, FilePath]):
        """
        Load data stored in `file_path` to a pipeline.
        The `file_path` must be a valid Ak file or directory, i.e. written by :py:class:`AkSinkBatchOp`.
        No program execution is triggered.

        :param file_path: a valid Ak file or directory storing the data.
        :return: a pipeline.
        """

        _j_pipeline_cls = get_java_class("com.alibaba.alink.pipeline.Pipeline")
        if isinstance(file_path, (str,)):
            path = file_path
            j_pipeline = _j_pipeline_cls.load(path)
        elif isinstance(file_path, FilePath):
            operator = file_path
            j_pipeline = _j_pipeline_cls.load(operator.get_j_obj())
        else:
            raise ValueError("file_path must be str or FilePath")

        stages = Pipeline._check_lazy_params(j_pipeline)
        return Pipeline(*stages, j_pipeline=j_pipeline)

    @staticmethod
    def _check_lazy_params(j_pipeline):
        j_op_type_util_cls = get_java_class("com.alibaba.alink.python.utils.OpTypeUtil")
        stages = []
        for i in range(j_pipeline.size()):
            j_stage = j_pipeline.get(i)
            params = Params.fromJson(j_stage.getParams().toJson())

            if j_op_type_util_cls.isEstimatorBase(j_stage):
                stage = Estimator.wrap_estimator(j_stage)
                if params.get(LAZY_PRINT_MODEL_INFO_ENABLED, False):
                    stage.enableLazyPrintModelInfo(params.get(LAZY_PRINT_MODEL_INFO_TITLE))
                if params.get(LAZY_PRINT_TRAIN_INFO_ENABLED, False):
                    stage.enableLazyPrintTrainInfo(params.get(LAZY_PRINT_TRAIN_INFO_TITLE))
            elif j_op_type_util_cls.isModelBase(j_stage):
                stage = Model.wrap_model(j_stage)
            elif j_op_type_util_cls.isTransformerBase(j_stage):
                stage = Transformer.wrap_transformer(j_stage)
            else:
                raise ValueError("stages are not correct.")

            if params.get(LAZY_PRINT_TRANSFORM_STAT_ENABLED, False):
                stage.enableLazyPrintTransformStat(params.get(LAZY_PRINT_TRANSFORM_STAT_TITLE))
            if params.get(LAZY_PRINT_TRANSFORM_DATA_ENABLED, False):
                stage.enableLazyPrintTransformData(
                    params.get(LAZY_PRINT_TRANSFORM_DATA_NUM),
                    params.get(LAZY_PRINT_TRANSFORM_DATA_TITLE))

            stages.append(stage)
        return stages


class PipelineWrapper(Pipeline):
    def __init__(self, j_pipeline):
        super(Estimator, self).__init__()
        self.op = j_pipeline


class TuningEvaluator(WithParams, ABC):
    """
    Evaluator in parameter tuning operators.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls_name = kwargs.pop('CLS_NAME', None)
        self.evaluator = get_java_class(cls_name)()

    def get_j_obj(self):
        return self.evaluator

    def evaluate(self, op: BatchOperator) -> float:
        """
        Obtain evaluation metric from prediction result `op`.

        :param op: prediction result.
        :return: evaluation metric.
        """
        return self.get_j_obj().evaluate(op)

    def isLargerBetter(self) -> bool:
        """
        Indicate whether larger evaluation metric is better or not.

        :return: whether larger evaluation metric is better.
        """
        return self.get_j_obj().isLargerBetter()

    def getMetricParamInfo(self) -> str:
        """
        Name of evaluation metric.

        :return: name of evaluation metric.
        """
        return self.get_j_obj().getMetricParamInfo().getName()
