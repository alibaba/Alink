# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Utilities for early stopping."""
"""
NOTE: it is a copy of tensorflow_estimator/python/estimator/early_stopping.py.
See `CHANGES` in code for all modifications.
"""

import collections
import operator
import os

import tensorflow as tf
from tensorflow.python.distribute import distribution_strategy_context
from tensorflow.python.framework import ops
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import init_ops
from tensorflow.python.ops import variable_scope
from tensorflow.python.platform import tf_logging
from tensorflow.python.training import basic_session_run_hooks
from tensorflow.python.training import session_run_hook
from tensorflow.python.training import training_util
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator import estimator as estimator_lib

_EVENT_FILE_GLOB_PATTERN = 'events.out.tfevents.*'


@estimator_export('estimator.experimental.make_early_stopping_hook')
def make_early_stopping_hook(estimator,
                             should_stop_fn,
                             run_every_secs=60,
                             run_every_steps=None):
    """Creates early-stopping hook.

    Returns a `SessionRunHook` that stops training when `should_stop_fn` returns
    `True`.

    Usage example:

    ```python
    estimator = ...
    hook = early_stopping.make_early_stopping_hook(
        estimator, should_stop_fn=make_stop_fn(...))
    train_spec = tf.estimator.TrainSpec(..., hooks=[hook])
    tf.estimator.train_and_evaluate(estimator, train_spec, ...)
    ```

    Caveat: Current implementation supports early-stopping both training and
    evaluation in local mode. In distributed mode, training can be stopped but
    evaluation (where it's a separate job) will indefinitely wait for new model
    checkpoints to evaluate, so you will need other means to detect and stop it.
    Early-stopping evaluation in distributed mode requires changes in
    `train_and_evaluate` API and will be addressed in a future revision.

    Args:
      estimator: A `tf.estimator.Estimator` instance.
      should_stop_fn: `callable`, function that takes no arguments and returns a
        `bool`. If the function returns `True`, stopping will be initiated by the
        chief.
      run_every_secs: If specified, calls `should_stop_fn` at an interval of
        `run_every_secs` seconds. Defaults to 60 seconds. Either this or
        `run_every_steps` must be set.
      run_every_steps: If specified, calls `should_stop_fn` every
        `run_every_steps` steps. Either this or `run_every_secs` must be set.

    Returns:
      A `SessionRunHook` that periodically executes `should_stop_fn` and initiates
      early stopping if the function returns `True`.

    Raises:
      TypeError: If `estimator` is not of type `tf.estimator.Estimator`.
      ValueError: If both `run_every_secs` and `run_every_steps` are set.
    """
    if not isinstance(estimator, estimator_lib.Estimator):
        raise TypeError('`estimator` must have type `tf.estimator.Estimator`. '
                        'Got: {}'.format(type(estimator)))

    if run_every_secs is not None and run_every_steps is not None:
        raise ValueError('Only one of `run_every_secs` and `run_every_steps` must '
                         'be set.')

    train_distribute = estimator.config.train_distribute
    mwms = ['CollectiveAllReduceStrategy', 'MultiWorkerMirroredStrategy']
    # CHANGES: previous implementation makes all `train_distribute` satisfy the condition.
    if train_distribute and any([train_distribute.__class__.__name__.startswith(
            strategy) for strategy in mwms]):
        if run_every_secs:
            raise ValueError('run_every_secs should not be set when using '
                             'MultiWorkerMirroredStrategy.')
        return _MultiWorkerEarlyStoppingHook(should_stop_fn, run_every_steps)

    if estimator.config.is_chief:
        return _StopOnPredicateHook(should_stop_fn, run_every_secs, run_every_steps)
    else:
        return _CheckForStoppingHook()


@estimator_export('estimator.experimental.stop_if_higher_hook')
def stop_if_higher_hook(estimator,
                        metric_name,
                        threshold,
                        eval_dir=None,
                        min_steps=0,
                        run_every_secs=60,
                        run_every_steps=None):
    """Creates hook to stop if the given metric is higher than the threshold.

    Usage example:

    ```python
    estimator = ...
    # Hook to stop training if accuracy becomes higher than 0.9.
    hook = early_stopping.stop_if_higher_hook(estimator, "accuracy", 0.9)
    train_spec = tf.estimator.TrainSpec(..., hooks=[hook])
    tf.estimator.train_and_evaluate(estimator, train_spec, ...)
    ```

    Caveat: Current implementation supports early-stopping both training and
    evaluation in local mode. In distributed mode, training can be stopped but
    evaluation (where it's a separate job) will indefinitely wait for new model
    checkpoints to evaluate, so you will need other means to detect and stop it.
    Early-stopping evaluation in distributed mode requires changes in
    `train_and_evaluate` API and will be addressed in a future revision.

    Args:
      estimator: A `tf.estimator.Estimator` instance.
      metric_name: `str`, metric to track. "loss", "accuracy", etc.
      threshold: Numeric threshold for the given metric.
      eval_dir: If set, directory containing summary files with eval metrics. By
        default, `estimator.eval_dir()` will be used.
      min_steps: `int`, stop is never requested if global step is less than this
        value. Defaults to 0.
      run_every_secs: If specified, calls `should_stop_fn` at an interval of
        `run_every_secs` seconds. Defaults to 60 seconds. Either this or
        `run_every_steps` must be set.
      run_every_steps: If specified, calls `should_stop_fn` every
        `run_every_steps` steps. Either this or `run_every_secs` must be set.

    Returns:
      An early-stopping hook of type `SessionRunHook` that periodically checks
      if the given metric is higher than specified threshold and initiates
      early stopping if true.
    """
    return _stop_if_threshold_crossed_hook(
        estimator=estimator,
        metric_name=metric_name,
        threshold=threshold,
        higher_is_better=True,
        eval_dir=eval_dir,
        min_steps=min_steps,
        run_every_secs=run_every_secs,
        run_every_steps=run_every_steps)


@estimator_export('estimator.experimental.stop_if_lower_hook')
def stop_if_lower_hook(estimator,
                       metric_name,
                       threshold,
                       eval_dir=None,
                       min_steps=0,
                       run_every_secs=60,
                       run_every_steps=None):
    """Creates hook to stop if the given metric is lower than the threshold.

    Usage example:

    ```python
    estimator = ...
    # Hook to stop training if loss becomes lower than 100.
    hook = early_stopping.stop_if_lower_hook(estimator, "loss", 100)
    train_spec = tf.estimator.TrainSpec(..., hooks=[hook])
    tf.estimator.train_and_evaluate(estimator, train_spec, ...)
    ```

    Caveat: Current implementation supports early-stopping both training and
    evaluation in local mode. In distributed mode, training can be stopped but
    evaluation (where it's a separate job) will indefinitely wait for new model
    checkpoints to evaluate, so you will need other means to detect and stop it.
    Early-stopping evaluation in distributed mode requires changes in
    `train_and_evaluate` API and will be addressed in a future revision.

    Args:
      estimator: A `tf.estimator.Estimator` instance.
      metric_name: `str`, metric to track. "loss", "accuracy", etc.
      threshold: Numeric threshold for the given metric.
      eval_dir: If set, directory containing summary files with eval metrics. By
        default, `estimator.eval_dir()` will be used.
      min_steps: `int`, stop is never requested if global step is less than this
        value. Defaults to 0.
      run_every_secs: If specified, calls `should_stop_fn` at an interval of
        `run_every_secs` seconds. Defaults to 60 seconds. Either this or
        `run_every_steps` must be set.
      run_every_steps: If specified, calls `should_stop_fn` every
        `run_every_steps` steps. Either this or `run_every_secs` must be set.

    Returns:
      An early-stopping hook of type `SessionRunHook` that periodically checks
      if the given metric is lower than specified threshold and initiates
      early stopping if true.
    """
    return _stop_if_threshold_crossed_hook(
        estimator=estimator,
        metric_name=metric_name,
        threshold=threshold,
        higher_is_better=False,
        eval_dir=eval_dir,
        min_steps=min_steps,
        run_every_secs=run_every_secs,
        run_every_steps=run_every_steps)


@estimator_export('estimator.experimental.stop_if_no_increase_hook')
def stop_if_no_increase_hook(estimator,
                             metric_name,
                             max_steps_without_increase,
                             eval_dir=None,
                             min_steps=0,
                             run_every_secs=60,
                             run_every_steps=None):
    """Creates hook to stop if metric does not increase within given max steps.

    Usage example:

    ```python
    estimator = ...
    # Hook to stop training if accuracy does not increase in over 100000 steps.
    hook = early_stopping.stop_if_no_increase_hook(estimator, "accuracy", 100000)
    train_spec = tf.estimator.TrainSpec(..., hooks=[hook])
    tf.estimator.train_and_evaluate(estimator, train_spec, ...)
    ```

    Caveat: Current implementation supports early-stopping both training and
    evaluation in local mode. In distributed mode, training can be stopped but
    evaluation (where it's a separate job) will indefinitely wait for new model
    checkpoints to evaluate, so you will need other means to detect and stop it.
    Early-stopping evaluation in distributed mode requires changes in
    `train_and_evaluate` API and will be addressed in a future revision.

    Args:
      estimator: A `tf.estimator.Estimator` instance.
      metric_name: `str`, metric to track. "loss", "accuracy", etc.
      max_steps_without_increase: `int`, maximum number of training steps with no
        increase in the given metric.
      eval_dir: If set, directory containing summary files with eval metrics. By
        default, `estimator.eval_dir()` will be used.
      min_steps: `int`, stop is never requested if global step is less than this
        value. Defaults to 0.
      run_every_secs: If specified, calls `should_stop_fn` at an interval of
        `run_every_secs` seconds. Defaults to 60 seconds. Either this or
        `run_every_steps` must be set.
      run_every_steps: If specified, calls `should_stop_fn` every
        `run_every_steps` steps. Either this or `run_every_secs` must be set.

    Returns:
      An early-stopping hook of type `SessionRunHook` that periodically checks
      if the given metric shows no increase over given maximum number of
      training steps, and initiates early stopping if true.
    """
    return _stop_if_no_metric_improvement_hook(
        estimator=estimator,
        metric_name=metric_name,
        max_steps_without_improvement=max_steps_without_increase,
        higher_is_better=True,
        eval_dir=eval_dir,
        min_steps=min_steps,
        run_every_secs=run_every_secs,
        run_every_steps=run_every_steps)


@estimator_export('estimator.experimental.stop_if_no_decrease_hook')
def stop_if_no_decrease_hook(estimator,
                             metric_name,
                             max_steps_without_decrease,
                             eval_dir=None,
                             min_steps=0,
                             run_every_secs=60,
                             run_every_steps=None):
    """Creates hook to stop if metric does not decrease within given max steps.

    Usage example:

    ```python
    estimator = ...
    # Hook to stop training if loss does not decrease in over 100000 steps.
    hook = early_stopping.stop_if_no_decrease_hook(estimator, "loss", 100000)
    train_spec = tf.estimator.TrainSpec(..., hooks=[hook])
    tf.estimator.train_and_evaluate(estimator, train_spec, ...)
    ```

    Caveat: Current implementation supports early-stopping both training and
    evaluation in local mode. In distributed mode, training can be stopped but
    evaluation (where it's a separate job) will indefinitely wait for new model
    checkpoints to evaluate, so you will need other means to detect and stop it.
    Early-stopping evaluation in distributed mode requires changes in
    `train_and_evaluate` API and will be addressed in a future revision.

    Args:
      estimator: A `tf.estimator.Estimator` instance.
      metric_name: `str`, metric to track. "loss", "accuracy", etc.
      max_steps_without_decrease: `int`, maximum number of training steps with no
        decrease in the given metric.
      eval_dir: If set, directory containing summary files with eval metrics. By
        default, `estimator.eval_dir()` will be used.
      min_steps: `int`, stop is never requested if global step is less than this
        value. Defaults to 0.
      run_every_secs: If specified, calls `should_stop_fn` at an interval of
        `run_every_secs` seconds. Defaults to 60 seconds. Either this or
        `run_every_steps` must be set.
      run_every_steps: If specified, calls `should_stop_fn` every
        `run_every_steps` steps. Either this or `run_every_secs` must be set.

    Returns:
      An early-stopping hook of type `SessionRunHook` that periodically checks
      if the given metric shows no decrease over given maximum number of
      training steps, and initiates early stopping if true.
    """
    return _stop_if_no_metric_improvement_hook(
        estimator=estimator,
        metric_name=metric_name,
        max_steps_without_improvement=max_steps_without_decrease,
        higher_is_better=False,
        eval_dir=eval_dir,
        min_steps=min_steps,
        run_every_secs=run_every_secs,
        run_every_steps=run_every_steps)


def read_eval_metrics(eval_dir):
    """Helper to read eval metrics from eval summary files.

    Args:
      eval_dir: Directory containing summary files with eval metrics.

    Returns:
      A `dict` with global steps mapping to `dict` of metric names and values.
    """
    eval_metrics_dict = collections.defaultdict(dict)
    for event in _summaries(eval_dir):
        if not event.HasField('summary'):
            continue
        metrics = {}
        for value in event.summary.value:
            if value.HasField('simple_value'):
                metrics[value.tag] = value.simple_value
        if metrics:
            eval_metrics_dict[event.step].update(metrics)
    return collections.OrderedDict(
        sorted(eval_metrics_dict.items(), key=lambda t: t[0]))


def _stop_if_threshold_crossed_hook(estimator, metric_name, threshold,
                                    higher_is_better, eval_dir, min_steps,
                                    run_every_secs, run_every_steps):
    """Creates early-stopping hook to stop training if threshold is crossed."""

    if eval_dir is None:
        eval_dir = estimator.eval_dir()

    is_lhs_better = operator.gt if higher_is_better else operator.lt
    greater_or_lesser = 'greater than' if higher_is_better else 'less than'

    def stop_if_threshold_crossed_fn():
        """Returns `True` if the given metric crosses specified threshold."""

        eval_results = read_eval_metrics(eval_dir)

        for step, metrics in eval_results.items():
            if step < min_steps:
                continue
            val = metrics[metric_name]
            if is_lhs_better(val, threshold):
                tf.compat.v1.logging.info(
                    'At step %s, metric "%s" has value %s which is %s the configured '
                    'threshold (%s) for early stopping.', step, metric_name, val,
                    greater_or_lesser, threshold)
                return True
        return False

    return make_early_stopping_hook(
        estimator=estimator,
        should_stop_fn=stop_if_threshold_crossed_fn,
        run_every_secs=run_every_secs,
        run_every_steps=run_every_steps)


def _stop_if_no_metric_improvement_hook(estimator, metric_name,
                                        max_steps_without_improvement,
                                        higher_is_better, eval_dir, min_steps,
                                        run_every_secs, run_every_steps):
    """Returns hook to stop training if given metric shows no improvement."""

    if eval_dir is None:
        eval_dir = estimator.eval_dir()

    is_lhs_better = operator.gt if higher_is_better else operator.lt
    increase_or_decrease = 'increase' if higher_is_better else 'decrease'

    def stop_if_no_metric_improvement_fn():
        """Returns `True` if metric does not improve within max steps."""

        eval_results = read_eval_metrics(eval_dir)

        best_val = None
        best_val_step = None
        for step, metrics in eval_results.items():
            if step < min_steps:
                continue
            val = metrics[metric_name]
            if best_val is None or is_lhs_better(val, best_val):
                best_val = val
                best_val_step = step
            if step - best_val_step >= max_steps_without_improvement:
                tf.compat.v1.logging.info(
                    'No %s in metric "%s" for %s steps, which is greater than or equal '
                    'to max steps (%s) configured for early stopping.',
                    increase_or_decrease, metric_name, step - best_val_step,
                    max_steps_without_improvement)
                return True
        return False

    return make_early_stopping_hook(
        estimator=estimator,
        should_stop_fn=stop_if_no_metric_improvement_fn,
        run_every_secs=run_every_secs,
        run_every_steps=run_every_steps)


def _summaries(eval_dir):
    """Yields `tensorflow.Event` protos from event files in the eval dir.

    Args:
      eval_dir: Directory containing summary files with eval metrics.

    Yields:
      `tensorflow.Event` object read from the event files.
    """
    if tf.compat.v1.gfile.Exists(eval_dir):
        for event_file in tf.compat.v1.gfile.Glob(
                os.path.join(eval_dir, _EVENT_FILE_GLOB_PATTERN)):
            for event in tf.compat.v1.train.summary_iterator(event_file):
                yield event


def _get_or_create_stop_var():
    with tf.compat.v1.variable_scope(
            name_or_scope='signal_early_stopping',
            values=[],
            reuse=tf.compat.v1.AUTO_REUSE):
        return tf.compat.v1.get_variable(
            name='STOP',
            shape=[],
            dtype=tf.dtypes.bool,
            initializer=tf.compat.v1.initializers.constant(False),
            collections=[tf.compat.v1.GraphKeys.GLOBAL_VARIABLES],
            trainable=False)


class _StopOnPredicateHook(tf.compat.v1.train.SessionRunHook):
    """Hook that requests stop when `should_stop_fn` returns `True`."""

    def __init__(self, should_stop_fn, run_every_secs=60, run_every_steps=None):
        if not callable(should_stop_fn):
            raise TypeError('`should_stop_fn` must be callable.')

        self._should_stop_fn = should_stop_fn
        self._timer = tf.compat.v1.train.SecondOrStepTimer(
            every_secs=run_every_secs, every_steps=run_every_steps)
        self._global_step_tensor = None
        self._stop_var = None
        self._stop_op = None

    def begin(self):
        self._global_step_tensor = tf.compat.v1.train.get_global_step()
        self._stop_var = _get_or_create_stop_var()
        self._stop_op = tf.compat.v1.assign(self._stop_var, True)

    def before_run(self, run_context):
        del run_context
        return tf.compat.v1.train.SessionRunArgs(self._global_step_tensor)

    def after_run(self, run_context, run_values):
        global_step = run_values.results
        if self._timer.should_trigger_for_step(global_step):
            self._timer.update_last_triggered_step(global_step)
            if self._should_stop_fn():
                tf.compat.v1.logging.info('Requesting early stopping at global step %d',
                                          global_step)
                run_context.session.run(self._stop_op)
                run_context.request_stop()


class _CheckForStoppingHook(tf.compat.v1.train.SessionRunHook):
    """Hook that requests stop if stop is requested by `_StopOnPredicateHook`."""

    def __init__(self):
        self._stop_var = None

    def begin(self):
        self._stop_var = _get_or_create_stop_var()

    def before_run(self, run_context):
        del run_context
        return tf.compat.v1.train.SessionRunArgs(self._stop_var)

    def after_run(self, run_context, run_values):
        should_early_stop = run_values.results
        if should_early_stop:
            tf.compat.v1.logging.info('Early stopping requested, suspending run.')
            run_context.request_stop()


class _MultiWorkerEarlyStoppingHook(session_run_hook.SessionRunHook):
    """Hook that requests stop when `should_stop_fn` returns `True`."""

    def _get_or_create_stop_var_with_aggregation(self):
        with variable_scope.variable_scope(
                name_or_scope='signal_early_stopping',
                values=[],
                reuse=variable_scope.AUTO_REUSE):
            return variable_scope.get_variable(
                name='STOP',
                shape=[],
                dtype=tf.dtypes.int32,
                initializer=init_ops.constant_initializer(0),
                collections=[ops.GraphKeys.GLOBAL_VARIABLES],
                synchronization=variable_scope.VariableSynchronization.ON_WRITE,
                aggregation=variable_scope.VariableAggregation.SUM,
                trainable=False)

    def __init__(self, should_stop_fn, run_every_steps=None):
        if not callable(should_stop_fn):
            raise TypeError('`should_stop_fn` must be callable.')

        self._should_stop_fn = should_stop_fn
        self._timer = basic_session_run_hooks.SecondOrStepTimer(
            every_secs=None, every_steps=run_every_steps)
        self._global_step_tensor = None
        self._stop_var = None
        self._stop_op = None
        self._non_stop_op = None

    def begin(self):
        self._global_step_tensor = training_util.get_global_step()
        self._stop_var = self._get_or_create_stop_var_with_aggregation()
        assert distribution_strategy_context.in_cross_replica_context()

        strategy = distribution_strategy_context.get_strategy()
        self._stop_placeholder = None

        def stop_op_fn(var):
            placeholder = array_ops.placeholder_with_default(
                0, tuple(), name='stop_value')
            if self._stop_placeholder is None:
                self._stop_placeholder = placeholder
            return var.assign_add(placeholder)

        self._stop_op = strategy.run(
            stop_op_fn, args=(self._stop_var,))

    def before_run(self, run_context):
        del run_context
        return session_run_hook.SessionRunArgs({
            'global_step': self._global_step_tensor,
            'stop_var': self._stop_var
        })

    def after_run(self, run_context, run_values):
        global_step = run_values.results['global_step']
        should_early_stop = run_values.results['stop_var']

        if should_early_stop > 0:
            tf_logging.info('Early stopping requested, suspending run.')
            run_context.request_stop()
            return
        if self._timer.should_trigger_for_step(global_step):
            self._timer.update_last_triggered_step(global_step)
            if self._should_stop_fn():
                run_context.session.run(
                    self._stop_op, feed_dict={self._stop_placeholder: 1})
                tf_logging.info('Requesting early stopping at global step %d',
                                global_step)
            else:
                run_context.session.run(
                    self._stop_op, feed_dict={self._stop_placeholder: 0})
