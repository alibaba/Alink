import warnings
from typing import Optional, Union

import deprecation
from py4j.java_gateway import JavaObject

from .lazy.has_lazy_print_transform_info import HasLazyPrintTransformInfo
from .local_predictor import LocalPredictable
from ..batch.base import BatchOperator, BatchOperatorWrapper
from ..common.types.bases.model_stream_scan_params import ModelStreamScanParams
from ..common.types.bases.params import Params
from ..common.types.bases.with_params import WithParams
from ..common.types.conversion.type_converters import py_list_to_j_array
from ..common.types.file_system.file_system import FilePath
from ..py4j_util import get_java_gateway, get_java_class
from ..stream.base import StreamOperatorWrapper

LAZY_PRINT_TRANSFORM_DATA_ENABLED = "lazyPrintTransformDataEnabled"
LAZY_PRINT_TRANSFORM_DATA_TITLE = "lazyPrintTransformDataTitle"
LAZY_PRINT_TRANSFORM_DATA_NUM = "lazyPrintTransformDataNum"
LAZY_PRINT_TRANSFORM_STAT_ENABLED = "lazyPrintTransformStatEnabled"
LAZY_PRINT_TRANSFORM_STAT_TITLE = "lazyPrintTransformStatTitle"

LAZY_PRINT_TRAIN_INFO_ENABLED = "lazyPrintTrainInfoEnabled"
LAZY_PRINT_TRAIN_INFO_TITLE = "lazyPrintTrainInfoTitle"

LAZY_PRINT_MODEL_INFO_ENABLED = "lazyPrintModelInfoEnabled"
LAZY_PRINT_MODEL_INFO_TITLE = "lazyPrintModelInfoTitle"


class Transformer(WithParams, HasLazyPrintTransformInfo):
    def get_j_obj(self):
        return self.j_transformer

    def __init__(self, j_transformer=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        clsName = kwargs.pop('CLS_NAME', None)
        self.opType = kwargs.pop('OP_TYPE', 'FUNCTION')
        if j_transformer is None:
            self.j_transformer = get_java_gateway().jvm.__getattr__(clsName)()
        else:
            self.j_transformer = j_transformer

    def transform(self, op):
        if isinstance(op, BatchOperator):
            return BatchOperatorWrapper(self.get_j_obj().transform(op.get_j_obj()))
        else:
            return StreamOperatorWrapper(self.get_j_obj().transform(op.get_j_obj()))

    @staticmethod
    def wrap_transformer(j_transformer: JavaObject):
        model_name = j_transformer.getClass().getSimpleName()
        import importlib
        model_cls = getattr(importlib.import_module("pyalink.alink.pipeline"), model_name)
        return model_cls(j_transformer=j_transformer)


class TransformerWrapper(Transformer):
    def __init__(self, j_transformer):
        super(TransformerWrapper, self).__init__(j_transformer=j_transformer)


class Model(Transformer):
    def __init__(self, j_model=None, *args, **kwargs):
        super(Model, self).__init__(j_transformer=j_model, *args, **kwargs)

    def getModelData(self):
        return BatchOperatorWrapper(self.get_j_obj().getModelData())

    def setModelData(self, model_data):
        self.get_j_obj().setModelData(model_data.get_j_obj())
        return self

    @staticmethod
    def wrap_model(j_model: JavaObject):
        model_name = j_model.getClass().getSimpleName()
        import importlib
        model_cls = getattr(importlib.import_module("pyalink.alink.pipeline"), model_name)
        return model_cls(j_model=j_model)


class PipelineModel(Model, ModelStreamScanParams, LocalPredictable):
    def __init__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_model = args[0]
        else:
            j_pipeline_model_cls = get_java_class("com.alibaba.alink.pipeline.PipelineModel")
            j_transformer_base_cls = get_java_class("com.alibaba.alink.pipeline.TransformerBase")
            j_transformers = list(map(lambda d: d.get_j_obj(), args))
            j_transformer_arr = py_list_to_j_array(j_transformer_base_cls, len(args), j_transformers)
            j_model = j_pipeline_model_cls(j_transformer_arr)
        super().__init__(j_model, **kwargs)

    def getTransformers(self):
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
        if file_path is None:
            return BatchOperatorWrapper(self.get_j_obj().save())
        elif isinstance(file_path, str):
            self.get_j_obj().save(file_path, overwrite)
        elif isinstance(file_path, FilePath):
            self.get_j_obj().save(file_path.get_j_obj(), overwrite, numFiles)
        else:
            raise ValueError("file_path must be str or FilePath")

    @staticmethod
    def collectLoad(operator: BatchOperator):
        _j_pipeline_model_cls = get_java_class("com.alibaba.alink.pipeline.PipelineModel")
        j_pipeline_model = _j_pipeline_model_cls.collectLoad(operator.get_j_obj())
        PipelineModel._check_lazy_params(j_pipeline_model)
        return PipelineModel(j_pipeline_model)

    @staticmethod
    def load(file_path: Union[str, FilePath]):
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
    def get_j_obj(self):
        return self._j_estimator

    def __init__(self, j_estimator=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        clsName = kwargs.pop('CLS_NAME', None)
        self.opType = kwargs.pop('OP_TYPE', 'FUNCTION')
        if j_estimator is None:
            self._j_estimator = get_java_gateway().jvm.__getattr__(clsName)()
        else:
            self._j_estimator = j_estimator

    def fit(self, op):
        j_model = self.get_j_obj().fit(op.get_j_obj())
        return Model.wrap_model(j_model)

    def fitAndTransform(self, op):
        if isinstance(op, BatchOperator):
            return BatchOperatorWrapper(self.get_j_obj().fitAndTransform(op.get_j_obj()))
        else:
            return StreamOperatorWrapper(self.get_j_obj().fitAndTransform(op.get_j_obj()))

    @staticmethod
    def wrap_estimator(j_estimator: JavaObject):
        model_name = j_estimator.getClass().getSimpleName()
        import importlib
        model_cls = getattr(importlib.import_module("pyalink.alink.pipeline"), model_name)
        return model_cls(j_estimator=j_estimator)


class EstimatorWrapper(Estimator):
    def __init__(self, j_estimator):
        super(EstimatorWrapper, self).__init__(j_estimator=j_estimator)


class Pipeline(Estimator):
    def __init__(self, *stages, j_pipeline=None):
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

    def add(self, *args):
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

    @deprecation.deprecated()
    def remove(self, index):
        self._j_pipeline.remove(index)
        return self._stages.pop(index)

    def get(self, index):
        return self._stages[index]

    def size(self):
        return len(self._stages)

    def fit(self, op):
        return PipelineModel(self._j_pipeline.fit(op.get_j_obj()))

    def save(self, file_path: Optional[Union[str, FilePath]] = None, overwrite: bool = False, numFiles: int = 1):
        if file_path is None:
            return BatchOperatorWrapper(self.get_j_obj().save())
        elif isinstance(file_path, str):
            self.get_j_obj().save(file_path)
        elif isinstance(file_path, FilePath):
            self.get_j_obj().save(file_path.get_j_obj(), overwrite, numFiles)
        else:
            raise ValueError("file_path must be str or FilePath")

    @staticmethod
    def collectLoad(operator: BatchOperator):
        _j_pipeline_cls = get_java_class("com.alibaba.alink.pipeline.Pipeline")
        j_pipeline = _j_pipeline_cls.collectLoad(operator.get_j_obj())
        stages = Pipeline._check_lazy_params(j_pipeline)
        return Pipeline(*stages, j_pipeline=j_pipeline)

    @staticmethod
    def load(file_path: Union[str, FilePath]):
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


class TuningEvaluator(WithParams):
    def get_j_obj(self):
        return self.evaluator

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls_name = kwargs.pop('CLS_NAME', None)
        self.evaluator = get_java_class(cls_name)()

    def evaluate(self, op):
        return self.get_j_obj().evaluate(op)

    def isLargerBetter(self):
        return self.get_j_obj().isLargerBetter()

    def getMetricParamInfo(self):
        return self.get_j_obj().getMetricParamInfo().getName()
