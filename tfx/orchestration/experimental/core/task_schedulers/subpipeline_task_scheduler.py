# Copyright 2022 Google LLC. All Rights Reserved.
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
"""A task scheduler for subpipeline."""

import copy
import time

from absl import logging
from tfx.orchestration import metadata
from tfx.orchestration.experimental.core import pipeline_ops
from tfx.orchestration.experimental.core import task as task_lib
from tfx.orchestration.experimental.core import task_scheduler
from tfx.proto.orchestration import pipeline_pb2
from tfx.utils import status as status_lib

from ml_metadata.proto import metadata_store_pb2

_POLLING_INTERVAL_SECS = 60


def is_execution_state_successful(
    state: metadata_store_pb2.Execution.State) -> bool:
  """Whether or not an execution state is successful."""
  return (state == metadata_store_pb2.Execution.COMPLETE or
          state == metadata_store_pb2.Execution.CACHED)


def is_execution_state_active(
    state: metadata_store_pb2.Execution.State) -> bool:
  """Returns `True` if an execution state is active."""
  return (state == metadata_store_pb2.Execution.NEW or
          state == metadata_store_pb2.Execution.RUNNING)


def is_execution_state_failed(
    state: metadata_store_pb2.Execution.State) -> bool:
  """Whether or not an execution state is failed."""
  return not is_execution_state_successful(
      state) and not is_execution_state_active(state)


class SubPipelineTaskScheduler(
    task_scheduler.TaskScheduler[task_lib.ExecNodeTask]):
  """A task scheduler for subpipeline."""

  def __init__(self, mlmd_handle: metadata.Metadata,
               pipeline: pipeline_pb2.Pipeline, task: task_lib.ExecNodeTask):
    super().__init__(mlmd_handle, pipeline, task)
    pipeline_node = self.task.get_pipeline_node()
    self._sub_pipeline = copy.deepcopy(
        _subpipeline_ir_rewrite(pipeline_node.raw_proto()))
    # The pipeline state is a context manager to get pipeline and node states.
    # It will be set when we start the pipeline.
    self._pipeline_state = None

  def schedule(self) -> task_scheduler.TaskSchedulerResult:
    try:
      self._pipeline_state = pipeline_ops.initiate_pipeline_start(
          self.mlmd_handle, self._sub_pipeline, None, None)
    except status_lib.StatusNotOkError as e:
      return task_scheduler.TaskSchedulerResult(
          status=e.status(), output=task_scheduler.ExecutorNodeOutput())

    # No need to hold the pipeline ops lock because we only read the states.
    while True:
      with self._pipeline_state:
        execution_state = self._pipeline_state.get_pipeline_execution_state()

      if is_execution_state_successful(execution_state):
        return task_scheduler.TaskSchedulerResult(
            status=status_lib.Status(code=status_lib.Code.OK),
            output=task_scheduler.ExecutorNodeOutput())
      if is_execution_state_failed(execution_state):
        return task_scheduler.TaskSchedulerResult(
            status=status_lib.Status(code=status_lib.Code.CANCELLED),
            output=task_scheduler.ExecutorNodeOutput())

      logging.info('Waiting for subpipeline %s to finish.',
                   self._pipeline_state.pipeline_uid)
      time.sleep(_POLLING_INTERVAL_SECS)

    # Should not reach here.
    raise RuntimeError(
        f'Subpipeline {self._pipeline_state.pipeline_uid} scheduling failed.')

  def cancel(self) -> None:
    if not self._pipeline_state:
      raise ValueError('Failed to find pipeline_state. '
                       'Have you called .schedule()?')
    logging.info('Received request to stop subpipeline; pipeline uid: %s',
                 self._pipeline_state.pipeline_uid)
    with pipeline_ops._PIPELINE_OPS_LOCK:  # pylint: disable=protected-access
      with self._pipeline_state:
        self._pipeline_state.initiate_stop(
            status_lib.Status(
                code=status_lib.Code.CANCELLED,
                message='Subpipeline cancellation requested by client.'))
    # No need to wait here. Because after the CANCELLED state being set,
    # the main loop in schedule() will pick up the state change and exit.


def _subpipeline_ir_rewrite(
    pipeline: pipeline_pb2.Pipeline) -> pipeline_pb2.Pipeline:
  # Clear the upstream nodes of PipelineBegin node and
  # downstream nodes of PipelineEnd node
  pipeline.nodes[0].pipeline_node.ClearField('upstream_nodes')
  pipeline.nodes[-1].pipeline_node.ClearField('downstream_nodes')
  return pipeline
