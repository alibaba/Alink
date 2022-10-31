import json
import os

import tensorflow as tf
from tensorflow.python.estimator.run_config import RunConfig

if tf.__version__ >= '2.0':
    tf = tf.compat.v1
    tf.compat.v1.disable_v2_behavior()


def generate_run_config(
        intra_op_parallelism_threads=4,
        inter_op_parallelism_threads=1,
        log_device_placement=False,
        save_checkpoints_steps=50,
        log_step_count_steps=10,
        keep_checkpoint_max=1,
        device_count=None,
        gpu_devices=None,
        **kwargs
) -> RunConfig:
    if gpu_devices is not None and len(gpu_devices) > 0:
        os.environ['CUDA_DEVICE_ORDER'] = 'PCI_BUS_ID'
        os.environ['CUDA_VISIBLE_DEVICES'] = ','.join(map(str, gpu_devices))
        print('Selecting GPU ID={}'.format(gpu_devices), flush=True)
        device_count = {'GPU': len(gpu_devices)}
        os.environ.pop('TF_CONFIG', None)
    elif device_count is None:
        device_count = {'CPU': 1}

    strategy = None
    device_filters = None

    if 'TF_CONFIG' not in os.environ:
        # single machine
        strategy = None
        if gpu_devices is not None and len(gpu_devices) > 1:
            devices = list(map(lambda d: "/gpu:" + str(d), gpu_devices))
            print(f'devices = {devices}', flush=True)
            strategy = tf.distribute.MirroredStrategy(devices=devices)
    else:
        tf_config = json.loads(os.environ['TF_CONFIG'])
        task_type = tf_config['task']['type']
        task_index = tf_config['task']['index']
        print("TF_CONFIG is", tf_config)

        if 'ps' in tf_config['cluster']:
            if tf.__version__ >= '2.0':
                strategy = tf.distribute.experimental.ParameterServerStrategy()
            else:
                # multiple workers, ps
                # disconnect all workers in ps mode.
                device_filters = ['/job:ps', '/job:%s/task:%d' % (task_type, task_index)]
        elif 'worker' in tf_config['cluster']:
            # multiple workers, allreduce
            strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
        else:
            # single worker, i.e., the chief worker
            strategy = None

    if tf.__version__ >= '2.0':
        session_config = tf.ConfigProto(
            intra_op_parallelism_threads=intra_op_parallelism_threads,
            inter_op_parallelism_threads=inter_op_parallelism_threads,
            allow_soft_placement=True,
            log_device_placement=log_device_placement)
        session_config.gpu_options.allow_growth = True
        config = tf.estimator.RunConfig(
            train_distribute=strategy,
            eval_distribute=None,
            session_config=session_config,
            save_checkpoints_steps=save_checkpoints_steps,
            log_step_count_steps=log_step_count_steps,
            keep_checkpoint_max=keep_checkpoint_max)
    else:
        session_config = tf.ConfigProto(
            device_count=device_count,
            intra_op_parallelism_threads=intra_op_parallelism_threads,
            inter_op_parallelism_threads=inter_op_parallelism_threads,
            allow_soft_placement=True,
            log_device_placement=log_device_placement,
            device_filters=device_filters)
        session_config.gpu_options.allow_growth = True
        config: RunConfig = tf.estimator.RunConfig(
            train_distribute=strategy,
            eval_distribute=None,
            session_config=session_config,
            save_checkpoints_steps=save_checkpoints_steps,
            log_step_count_steps=log_step_count_steps,
            keep_checkpoint_max=keep_checkpoint_max)
    print(f'run config = {config}', flush=True)

    return config
