import json
from typing import Dict, Union, Any

from py4j.java_gateway import JavaObject

from .j_obj_wrapper import JavaObjectWrapper


class Params(JavaObjectWrapper):
    _j_cls_name = "org.apache.flink.ml.api.misc.param.Params"

    def __init__(self, j_params: JavaObject = None):
        self._p = dict()
        if j_params is not None:
            self._p = json.loads(j_params.toJson())

    def get_j_obj(self) -> JavaObject:
        return self._j_cls().fromJson(self.toJson())

    def toJson(self):
        return json.dumps(self._p)

    @classmethod
    def fromJson(cls, data):
        x = Params()
        x._p = json.loads(data)
        return x

    def get(self, *args):
        """
        Params.get(key [, defaultValue])
        """
        if len(args) == 1:
            return json.loads(self._p[args[0]])
        else:
            key, val = args[:2]
            return json.loads(self._p[key]) if key in self._p else val

    def set(self, key: str, value: Any):
        self._p[key] = json.dumps(value)
        return self

    def __contains__(self, key):
        return key in self._p

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)
        return value

    def __delitem__(self, key):
        return self._p.pop(key)

    def __len__(self):
        return len(self._p)

    def __str__(self):
        return str(self._p)

    def contains(self, *key):
        return all(x in self._p for x in key)

    def remove(self, key):
        del self._p[key]
        return self

    def merge(self, other: 'Params'):
        for k, v in other._p.items():
            self._p[k] = v

    def items(self):
        return [(x, self.get(x)) for x in self._p.keys()]

    @classmethod
    def from_args(cls, params: Union['Params', Dict] = None, **kwargs) -> 'Params':
        obj = Params()
        if params is not None:
            if isinstance(params, (Params,)):
                obj.merge(params)
            elif isinstance(params, (dict,)):
                obj = Params()
                for k, v in params.items():
                    obj[k] = v
            else:
                raise TypeError("Invalid type for params")
        for k, v in kwargs.items():
            obj[k] = v
        return obj
