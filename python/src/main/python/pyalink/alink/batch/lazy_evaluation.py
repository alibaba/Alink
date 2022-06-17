from abc import ABC, abstractmethod

from rx import operators
from rx.subject import Subject

from ..py4j_util import get_java_class


class LazyEvaluation:
    def __init__(self, sub=None):
        self._done = False
        self._sub = sub if sub is not None else Subject()
        self._value = None
        self._sub.subscribe(self._add_value)

    def transform(self, fn):
        return LazyEvaluation(self._sub.pipe(operators.map(fn)))

    def addCallback(self, fn):
        self._sub.subscribe(fn)

    def _add_value(self, v):
        self._value = v
        self._done = True

    def add_value(self, v):
        self._sub.on_next(v)

    def get_latest_value(self):
        if self._done:
            return self._value
        else:
            raise ValueError("Result not set")


class PyConsumer(ABC):

    @abstractmethod
    def accept(self, obj):
        pass

    class Java:
        implements = ["java.util.function.Consumer"]


class PipeLazyEvaluationConsumer(PyConsumer):

    def __init__(self, lazy_eval):
        self.lazy_eval = lazy_eval

    def accept(self, obj):
        self.lazy_eval.add_value(obj)


def to_j_consumer_list(*callbacks):
    j_consumers = get_java_class("java.util.ArrayList")()
    for callback in callbacks:
        j_consumers.append(callback)
    return j_consumers


def pipe_j_lazy_to_py_callbacks(j_lazy_collect_f, py_callbacks, converter):
    """
    Call a Java lazy collect method with Python callbacks.
    When calling Python callbacks, the value is converted to Python one using provided converter.
    :param j_lazy_collect_f: a Java lazy collect method
    :param py_callbacks: callbacks written in Python
    :param converter: a value converter which converts the value from Java to Python
    :return:
    """

    def call_py_callbacks(j_val):
        py_val = converter(j_val)
        for callback in py_callbacks:
            callback(py_val)

    lazy_j_val = LazyEvaluation()
    lazy_j_val.addCallback(call_py_callbacks)

    pipe_consumer = PipeLazyEvaluationConsumer(lazy_j_val)
    j_consumers = get_java_class("java.util.ArrayList")()
    j_consumers.append(pipe_consumer)
    j_lazy_collect_f(j_consumers)
