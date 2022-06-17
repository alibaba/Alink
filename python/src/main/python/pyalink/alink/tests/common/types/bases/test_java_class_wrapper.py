import unittest

from py4j.java_gateway import JavaClass

from pyalink.alink import *
from pyalink.alink.common.types.bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion
from pyalink.alink.common.types.conversion.type_converters import get_all_subclasses
from pyalink.alink.py4j_util import get_java_class


class TestJavaClassWrapper(unittest.TestCase):

    def test_existence_of_java_class(self):
        py_classes = get_all_subclasses(JavaObjectWrapperWithAutoTypeConversion)
        not_java_class = []
        for py_class in py_classes:
            j_cls = py_class._j_cls()
            if not isinstance(j_cls, JavaClass):
                not_java_class.append(py_class._j_cls_name)
        self.assertTrue(len(not_java_class) == 0, "{} are not Java classes.".format(not_java_class))

    def test_existence_of_java_class_of_catalog(self):
        py_classes = get_all_subclasses(BaseCatalog)
        not_java_class = []
        for py_class in py_classes:
            j_cls = py_class._j_cls()
            if not isinstance(j_cls, JavaClass):
                not_java_class.append(py_class._j_cls_name)
        self.assertTrue(len(not_java_class) == 0, "{} are not Java classes.".format(not_java_class))

    def test_existence_of_java_class_method(self):
        AlinkGlobalConfiguration.setPrintProcessInfo(True)
        j_check_wrapper_util_cls = get_java_class("com.alibaba.alink.python.utils.CheckWrapperUtil")
        py_classes = get_all_subclasses(JavaObjectWrapperWithAutoTypeConversion)
        inconsistency = []
        for py_class in py_classes:
            j_cls_name = py_class._j_cls_name
            py_func_set = set(filter(
                lambda d: not d.startswith("_") and d != "get_j_obj" and d != "j_cls_name",
                dir(py_class)))
            if hasattr(py_class, '_unsupported_j_methods'):
                py_func_set = py_func_set.union(set(py_class._unsupported_j_methods))

            j_func_set = set(j_check_wrapper_util_cls.getJMethodNames(j_cls_name))

            if py_func_set != j_func_set:
                inconsistency.append((j_cls_name, py_func_set - j_func_set, j_func_set - py_func_set))
        self.assertTrue(len(inconsistency) == 0, "Inconsistency: {}".format(inconsistency))
