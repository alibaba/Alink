# Copyright 2019 The flink-ai-extended Authors. All Rights Reserved.
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
# =============================================================================

from __future__ import absolute_import
from __future__ import division
from __future__ import nested_scopes
from __future__ import print_function

import ctypes as ct
import logging
import platform
import random
import subprocess
import time

MAX_RETRIES = 3  #: Maximum retries to allocate GPUs


def _get_gpu():
    """*DEPRECATED*. Allocates first available GPU using cudaSetDevice(), or returns 0 otherwise."""
    # Note: this code executes, but Tensorflow subsequently complains that the "current context was not created by the StreamExecutor cuda_driver API"
    system = platform.system()
    if system == "Linux":
        libcudart = ct.cdll.LoadLibrary("libcudart.so")
    elif system == "Darwin":
        libcudart = ct.cdll.LoadLibrary("libcudart.dylib")
    elif system == "Windows":
        libcudart = ct.windll.LoadLibrary("libcudart.dll")
    else:
        raise NotImplementedError("Cannot identify system.")

    device_count = ct.c_int()
    libcudart.cudaGetDeviceCount(ct.byref(device_count))
    gpu = 0
    for i in range(device_count.value):
        if (0 == libcudart.cudaSetDevice(i) and 0 == libcudart.cudaFree(0)):
            gpu = i
            break
    return gpu


def get_gpus(num_gpu=1, worker_index=-1):
    """Get list of free GPUs according to nvidia-smi.

    This will retry for ``MAX_RETRIES`` times until the requested number of GPUs are available.

    Args:
      :num_gpu: number of GPUs desired.
      :worker_index: index "hint" for allocation of available GPUs.

    Returns:
      Comma-delimited string of GPU ids, or raises an Exception if the requested number of GPUs could not be found.
    """
    try:
        # get list of gpus (index, uuid)
        list_gpus = subprocess.check_output(["nvidia-smi", "--list-gpus"]).decode()
        logging.debug("all GPUs:\n{0}".format(list_gpus))

        # parse index and guid
        gpus = [x for x in list_gpus.split('\n') if len(x) > 0]
    except Exception as e:
        gpus = []
        return gpus

    def parse_gpu(gpu_str):
        cols = gpu_str.split(' ')
        return cols[5].split(')')[0], cols[1].split(':')[0]

    gpu_list = [parse_gpu(gpu) for gpu in gpus]

    free_gpus = []
    retries = 0
    while len(free_gpus) < num_gpu and retries < MAX_RETRIES:
        smi_output = subprocess.check_output(
            ["nvidia-smi", "--format=csv,noheader,nounits", "--query-compute-apps=gpu_uuid"]).decode()
        logging.debug("busy GPUs:\n{0}".format(smi_output))
        busy_uuids = [x for x in smi_output.split('\n') if len(x) > 0]
        for uuid, index in gpu_list:
            if uuid not in busy_uuids:
                free_gpus.append(index)

        if len(free_gpus) < num_gpu:
            logging.warn("Unable to find available GPUs: requested={0}, available={1}".format(num_gpu, len(free_gpus)))
            retries += 1
            time.sleep(30 * retries)
            free_gpus = []

    logging.info("Available GPUs: {}".format(free_gpus))

    # if still can't find available GPUs, raise exception
    if len(free_gpus) < num_gpu:
        smi_output = subprocess.check_output(
            ["nvidia-smi", "--format=csv", "--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory"]).decode()
        logging.info(": {0}".format(smi_output))
        raise Exception("Unable to find {} free GPU(s)\n{}".format(num_gpu, smi_output))

    # Get logical placement
    num_available = len(free_gpus)
    if worker_index == -1:
        # use original random placement
        random.shuffle(free_gpus)
        proposed_gpus = free_gpus[:num_gpu]
    else:
        # ordered by worker index
        if worker_index + num_gpu > num_available:
            worker_index = worker_index % num_available
        proposed_gpus = free_gpus[worker_index:(worker_index + num_gpu)]
    logging.info("Proposed GPUs: {}".format(proposed_gpus))

    # return ','.join(str(x) for x in proposed_gpus)
    return proposed_gpus


# Function to get the gpu information
def _get_free_gpu(max_gpu_utilization=40, min_free_memory=0.5, num_gpu=1):
    """Get available GPUs according to utilization thresholds.

    Args:
      :max_gpu_utilization: percent utilization threshold to consider a GPU "free"
      :min_free_memory: percent free memory to consider a GPU "free"
      :num_gpu: number of requested GPUs

    Returns:
      A tuple of (available_gpus, minimum_free_memory), where available_gpus is a comma-delimited string of GPU ids, and minimum_free_memory
      is the lowest amount of free memory available on the available_gpus.

    """

    def get_gpu_info():
        # Get the gpu information
        gpu_info = subprocess.check_output(["nvidia-smi", "--format=csv,noheader,nounits",
                                            "--query-gpu=index,memory.total,memory.free,memory.used,utilization.gpu"]).decode()
        gpu_info = gpu_info.split('\n')

        gpu_info_array = []

        # Check each gpu
        for line in gpu_info:
            if len(line) > 0:
                gpu_id, total_memory, free_memory, used_memory, gpu_util = line.split(',')
                gpu_memory_util = float(used_memory) / float(total_memory)
                gpu_info_array.append((float(gpu_util), gpu_memory_util, gpu_id))

        return (gpu_info_array)

    # Read the gpu information multiple times
    num_times_to_average = 5
    current_array = []
    for ind in range(num_times_to_average):
        current_array.append(get_gpu_info())
        time.sleep(1)

    # Get number of gpus
    num_gpus = len(current_array[0])

    # Average the gpu information
    avg_array = [(0, 0, str(x)) for x in range(num_gpus)]
    for ind in range(num_times_to_average):
        for gpu_ind in range(num_gpus):
            avg_array[gpu_ind] = (avg_array[gpu_ind][0] + current_array[ind][gpu_ind][0],
                                  avg_array[gpu_ind][1] + current_array[ind][gpu_ind][1], avg_array[gpu_ind][2])

    for gpu_ind in range(num_gpus):
        avg_array[gpu_ind] = (
        float(avg_array[gpu_ind][0]) / num_times_to_average, float(avg_array[gpu_ind][1]) / num_times_to_average,
        avg_array[gpu_ind][2])

    avg_array.sort()

    gpus_found = 0
    gpus_to_use = ""
    free_memory = 1.0
    # Return the least utilized GPUs if it's utilized less than max_gpu_utilization and amount of free memory is at least min_free_memory
    # Otherwise, run in cpu only mode
    for current_gpu in avg_array:
        if current_gpu[0] < max_gpu_utilization and (1 - current_gpu[1]) > min_free_memory:
            if gpus_found == 0:
                gpus_to_use = current_gpu[2]
                free_memory = 1 - current_gpu[1]
            else:
                gpus_to_use = gpus_to_use + "," + current_gpu[2]
                free_memory = min(free_memory, 1 - current_gpu[1])

            gpus_found = gpus_found + 1

        if gpus_found == num_gpu:
            break

    return gpus_to_use, free_memory
