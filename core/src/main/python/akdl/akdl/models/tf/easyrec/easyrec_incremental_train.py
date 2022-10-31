import easy_rec
import json
import logging
import math
import os
import tensorflow as tf
from akdl.runner.config import StreamTaskConfig
from easy_rec.python.builders import strategy_builder
from easy_rec.python.main import _create_estimator, _check_model_dir, FinalExporter, \
    LatestExporter, BestExporter
from easy_rec.python.protos import pipeline_pb2
from easy_rec.python.protos.train_pb2 import DistributionStrategy
from easy_rec.python.utils import config_util, load_class, estimator_utils
from easy_rec.python.utils.pai_util import is_on_pai
from google.protobuf import text_format
# noinspection PyUnresolvedReferences
from tensorflow_io.core.python.ops import core_ops

from .easyrec_incremental_train_hooks import OutputModelHook
from .easyrec_tfrecord_dataset_input import TFRecordDataSetInput

if tf.__version__ >= '2.0':
    gfile = tf.compat.v1.gfile
    from tensorflow.core.protobuf import config_pb2

    ConfigProto = config_pb2.ConfigProto
    GPUOptions = config_pb2.GPUOptions
else:
    gfile = tf.gfile
    GPUOptions = tf.GPUOptions
    ConfigProto = tf.ConfigProto

load_class.auto_import()


def _get_input_fn(data_config, feature_configs, export_config=None, tf_context=None):
    """Build estimator input function.

    Args:
      data_config:  dataset config
      feature_configs: FeatureConfig
      data_path: input_data_path
      export_config: configuration for exporting models,
        only used to build input_fn when exporting models

    Returns:
      subclass of Input
      :param tf_context: 
    """
    task_id, task_num = estimator_utils.get_task_index_and_num()
    input_obj = TFRecordDataSetInput(
        data_config,
        feature_configs,
        task_index=task_id,
        task_num=task_num,
        tf_context=tf_context)
    input_fn = input_obj.create_input(export_config)
    return input_fn


def _create_eval_export_spec(pipeline_config, tf_context=None):
    data_config = pipeline_config.data_config
    feature_configs = pipeline_config.feature_configs
    eval_config = pipeline_config.eval_config
    export_config = pipeline_config.export_config
    if eval_config.num_examples > 0:
        eval_steps = int(
            math.ceil(float(eval_config.num_examples) / data_config.batch_size))
        logging.info('eval_steps = %d' % eval_steps)
    else:
        eval_steps = None
    # create eval input
    export_input_fn = _get_input_fn(data_config, feature_configs, None,
                                    export_config)
    if export_config.exporter_type == 'final':
        exporters = [
            FinalExporter(name='final', serving_input_receiver_fn=export_input_fn)
        ]
    elif export_config.exporter_type == 'latest':
        exporters = [
            LatestExporter(
                name='latest',
                serving_input_receiver_fn=export_input_fn,
                exports_to_keep=export_config.exports_to_keep)
        ]
    elif export_config.exporter_type == 'best':
        logging.info(
            'will use BestExporter, metric is %s, the bigger the better: %d' %
            (export_config.best_exporter_metric, export_config.metric_bigger))

        def _metric_cmp_fn(best_eval_result, current_eval_result):
            logging.info('metric: best = %s current = %s' %
                         (str(best_eval_result), str(current_eval_result)))
            if export_config.metric_bigger:
                return (best_eval_result[export_config.best_exporter_metric] <
                        current_eval_result[export_config.best_exporter_metric])
            else:
                return (best_eval_result[export_config.best_exporter_metric] >
                        current_eval_result[export_config.best_exporter_metric])

        exporters = [
            BestExporter(
                name='best',
                serving_input_receiver_fn=export_input_fn,
                compare_fn=_metric_cmp_fn,
                exports_to_keep=export_config.exports_to_keep)
        ]
    elif export_config.exporter_type == 'none':
        exporters = []
    else:
        raise ValueError('Unknown exporter type %s' % export_config.exporter_type)

    # set throttle_secs to a small number, so that we can control evaluation
    # interval steps by checkpoint saving steps
    eval_input_fn = _get_input_fn(data_config, feature_configs, tf_context=tf_context)
    eval_spec = tf.estimator.EvalSpec(
        name='val',
        input_fn=lambda: eval_input_fn().take(0),
        steps=eval_steps,
        throttle_secs=10,
        exporters=exporters)
    return eval_spec


def _incremental_train_and_evaluate_impl(pipeline_config, tf_context, output_model_hook: OutputModelHook):
    continue_train = True
    # Tempoary for EMR
    if (not is_on_pai()) and 'TF_CONFIG' in os.environ:
        tf_config = json.loads(os.environ['TF_CONFIG'])
        # for ps on emr currently evaluator is not supported
        # the cluster has one chief instead of master
        # evaluation will not be done, so we replace chief with master
        if 'cluster' in tf_config and 'chief' in tf_config[
            'cluster'] and 'ps' in tf_config['cluster'] and (
                'evaluator' not in tf_config['cluster']):
            chief = tf_config['cluster']['chief']
            del tf_config['cluster']['chief']
            tf_config['cluster']['master'] = chief
            if tf_config['task']['type'] == 'chief':
                tf_config['task']['type'] = 'master'
            os.environ['TF_CONFIG'] = json.dumps(tf_config)

    train_config = pipeline_config.train_config
    data_config = pipeline_config.data_config
    feature_configs = pipeline_config.feature_configs

    if train_config.train_distribute != DistributionStrategy.NoStrategy \
            and train_config.sync_replicas:
        logging.warning(
            'will set sync_replicas to False, because train_distribute[%s] != NoStrategy'
            % pipeline_config.train_config.train_distribute)
        pipeline_config.train_config.sync_replicas = False

    # if pipeline_config.WhichOneof('train_path') == 'kafka_train_input':
    #     train_data = pipeline_config.kafka_train_input
    # else:
    #     train_data = pipeline_config.train_input_path
    #
    # if pipeline_config.WhichOneof('eval_path') == 'kafka_eval_input':
    #     eval_data = pipeline_config.kafka_eval_input
    # else:
    #     eval_data = pipeline_config.eval_input_path

    export_config = pipeline_config.export_config
    if export_config.dump_embedding_shape:
        embed_shape_dir = os.path.join(pipeline_config.model_dir,
                                       'embedding_shapes')
        if not gfile.Exists(embed_shape_dir):
            gfile.MakeDirs(embed_shape_dir)
        easy_rec._global_config['dump_embedding_shape_dir'] = embed_shape_dir
        pipeline_config.train_config.separate_save = True

    distribution = strategy_builder.build(train_config)
    estimator, run_config = _create_estimator(
        pipeline_config, distribution=distribution)

    master_stat_file = os.path.join(pipeline_config.model_dir, 'master.stat')
    version_file = os.path.join(pipeline_config.model_dir, 'version')
    if estimator_utils.is_chief():
        _check_model_dir(pipeline_config.model_dir, continue_train)
        config_util.save_pipeline_config(pipeline_config, pipeline_config.model_dir)
        with gfile.GFile(version_file, 'w') as f:
            f.write(easy_rec.__version__ + '\n')
        if gfile.Exists(master_stat_file):
            gfile.Remove(master_stat_file)

    train_steps = pipeline_config.train_config.num_steps
    if train_steps <= 0:
        train_steps = None
        logging.warn('will train INFINITE number of steps')
    else:
        logging.info('train_steps = %d' % train_steps)
    # create train input
    train_input_fn = _get_input_fn(data_config, feature_configs, tf_context=tf_context)

    # Currently only a single Eval Spec is allowed.

    # Add hook for periodically write back models to Alink
    print(f'data_config = {data_config}, '
          f'feature_configs = {feature_configs}, '
          f'export_config = {export_config}',
          flush=True)
    export_input_fn = _get_input_fn(data_config, feature_configs, export_config, tf_context=tf_context)
    output_model_hook.set_estimator_and_serving_fn(estimator, export_input_fn)

    train_spec = tf.estimator.TrainSpec(
        input_fn=train_input_fn, max_steps=train_steps, hooks=[output_model_hook])
    # create eval spec
    eval_spec = _create_eval_export_spec(pipeline_config, tf_context)
    tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)
    # estimator.train(input_fn=train_input_fn, max_steps=train_steps, hooks=[output_model_hook])
    logging.info('Train and evaluate finish')


def main(args: StreamTaskConfig):
    print(args)
    work_dir = args.work_dir
    writer = args.output_writer
    tf_context = args.tf_context
    user_params: dict = args.user_params
    task_type = args.task_type
    task_index = args.task_index

    checkpoint_path: str = user_params.get("ALINK:checkpoint_path")
    if not checkpoint_path.endswith("/"):
        checkpoint_path += "/"
    pipeline_config_path = checkpoint_path + "pipeline.config"

    # pipeline_config_path = user_params.get("ALINK:easyrec_config")
    pipeline_config = pipeline_pb2.EasyRecConfig()
    with gfile.GFile(pipeline_config_path, 'r') as f:
        config_str = f.read()
        text_format.Merge(config_str, pipeline_config)

    output_model_secs = user_params.get("ALINK:output_model_secs", pipeline_config.train_config.save_checkpoints_secs)
    output_model_secs = 30

    config_json = {
        "export_config.exporter_type": "none",
        "export_config.dump_embedding_shape": "none",
        "export_config.multi_placeholder": "none",
        "train_config.num_steps": 0,
        "data_config.num_epochs": 1,
        "eval_config.num_examples": 0,
        "train_config.save_checkpoints_secs": output_model_secs
    }

    output_model_hook: OutputModelHook = OutputModelHook(
        task_type == 'chief' and task_index == 0,
        writer,
        os.path.join(work_dir, "__local_export"),
        output_model_secs
    )

    config_util.edit_config(pipeline_config, config_json)
    config_util.auto_expand_share_feature_configs(pipeline_config)
    _incremental_train_and_evaluate_impl(pipeline_config, tf_context, output_model_hook)
