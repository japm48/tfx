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
"""Test pipeline for channels with optional / allow_empty."""

import os
from typing import Any, Dict, List

from tfx import types
from tfx.dsl.components.base import base_component
from tfx.dsl.components.base import base_executor
from tfx.dsl.components.base import executor_spec
from tfx.orchestration import pipeline
from tfx.types import component_spec
from tfx.types import standard_artifacts

_pipeline_name = 'optional_and_allow_empty_pipeline'
_pipeline_root = os.path.join('pipeline', _pipeline_name)


class MyComponentSpec(types.ComponentSpec):
  """ComponentSpec for MyComponent."""
  PARAMETERS = {}
  INPUTS = {
      'mandatory':
          component_spec.ChannelParameter(type=standard_artifacts.Model),
      'optional_but_needed':
          component_spec.ChannelParameter(
              type=standard_artifacts.Model, optional=True),
      'optional_and_not_needed':
          component_spec.ChannelParameter(
              type=standard_artifacts.Model, optional=True, allow_empty=True),
      'second_optional_but_needed':
          component_spec.ChannelParameter(
              type=standard_artifacts.Model, optional=True),
      'second_optional_and_not_needed':
          component_spec.ChannelParameter(
              type=standard_artifacts.Model, optional=True, allow_empty=True),
  }
  OUTPUTS = {}


class Executor(base_executor.BaseExecutor):
  """Executor for test component."""

  def Do(self, input_dict: Dict[str, List[types.Artifact]],
         output_dict: Dict[str, List[types.Artifact]],
         exec_properties: Dict[str, Any]) -> None:
    return


class MyComponent(base_component.BaseComponent):
  """MyComponent."""
  SPEC_CLASS = MyComponentSpec
  EXECUTOR_SPEC = executor_spec.ExecutorClassSpec(Executor)

  def __init__(self):
    spec = MyComponentSpec(
        mandatory=types.Channel(type=standard_artifacts.Model),
        optional_but_needed=types.Channel(type=standard_artifacts.Model),
        optional_and_not_needed=types.Channel(type=standard_artifacts.Model),
        # We don't provide the the two second_* channels.
    )
    super().__init__(spec=spec)


def create_test_pipeline():
  return pipeline.Pipeline(
      pipeline_name=_pipeline_name,
      pipeline_root=_pipeline_root,
      components=[MyComponent()])
