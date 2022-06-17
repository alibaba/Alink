Pipeline Operators
------------------

Base classes and mixins
^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: pyalink.alink.pipeline.base
    :show-inheritance:
    :members:
    :undoc-members:

.. automodule:: pyalink.alink.pipeline.mixins
    :show-inheritance:
    :members:
    :undoc-members:

All pipeline operators
^^^^^^^^^^^^^^^^^^^^^^

.. note::
    Most pipeline operators instance only support parameter setters and functions defined in :py:class:`Trainer` and :py:class:`Transformer`.
    Check :ref:`references of all pipeline operators <Ref of all pipeline operators>`.
    For more detailed explanation of operator usages and parameters, please check `Yuque Doc <https://www.yuque.com/pinshu/alink_doc/>`_.

    There are a few operators which provide more functionalities or have special usage in Python language.
    These operators and their APIs are shown below.

Python-specific operators
^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: pyalink.alink.pipeline.special_operators
    :show-inheritance:
    :members:
    :undoc-members:

Operators with additional APIs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: pyalink.alink.pipeline.models_with_methods
    :show-inheritance:
    :members:
    :undoc-members:

Parameter tuning
^^^^^^^^^^^^^^^^

.. automodule:: pyalink.alink.pipeline.tuning.param_dist
    :show-inheritance:
    :members:
    :undoc-members:

.. automodule:: pyalink.alink.pipeline.tuning.param_grid
    :show-inheritance:
    :members:
    :undoc-members:

.. automodule:: pyalink.alink.pipeline.tuning.base
    :show-inheritance:
    :members:
    :undoc-members:

.. note::
    Currently supported tuning operators are listed below.
    For more detailed explanation of operator usages and parameters, please check `Yuque Doc <https://www.yuque.com/pinshu/alink_doc/>`_.

.. autoclass:: pyalink.alink.pipeline.common.RandomSearchCV
    :show-inheritance:
    :members:
    :undoc-members:
    :exclude-members: __init__,CLS_NAME,OP_TYPE

.. autoclass:: pyalink.alink.pipeline.common.RandomSearchTVSplit
    :show-inheritance:
    :members:
    :undoc-members:
    :exclude-members: __init__,CLS_NAME,OP_TYPE

.. autoclass:: pyalink.alink.pipeline.common.GridSearchCV
    :show-inheritance:
    :members:
    :undoc-members:
    :exclude-members: __init__,CLS_NAME,OP_TYPE

.. autoclass:: pyalink.alink.pipeline.common.GridSearchTVSplit
    :show-inheritance:
    :members:
    :undoc-members:
    :exclude-members: __init__,CLS_NAME,OP_TYPE
