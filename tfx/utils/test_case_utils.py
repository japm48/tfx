# Copyright 2020 Google LLC. All Rights Reserved.
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
"""Utilities for customizing tf.test.TestCase class."""
from __future__ import annotations

import contextlib
import copy
import os
from typing import Dict, Iterable, Optional, Union, Mapping, Sequence, cast
import unittest

import tensorflow as tf
from tfx import types
from tfx.dsl.io import fileio
from tfx.orchestration import data_types_utils
from tfx.orchestration import metadata
from tfx.orchestration import mlmd_connection_manager as mlmd_cm
from tfx.orchestration.portable.mlmd import event_lib
from tfx.utils import io_utils
from tfx.utils import json_utils

from google.protobuf import message
from google.protobuf import text_format
from ml_metadata.proto import metadata_store_pb2


_ArtifactMultiMap = Mapping[str, Sequence[metadata_store_pb2.Artifact]]


@contextlib.contextmanager
def override_env_var(name: str, value: str):
  """Overrides an environment variable and returns a context manager.

  Example:
    with test_case_utils.override_env_var('HOME', new_home_dir):

    or

    self.enter_context(test_case_utils.override_env_var('HOME', new_home_dir))

  Args:
    name: Name of the environment variable.
    value: Overriding value.

  Yields:
    None.
  """
  old_value = os.getenv(name)
  os.environ[name] = value

  yield

  if old_value is None:
    del os.environ[name]
  else:
    os.environ[name] = old_value


class TfxTest(tf.test.TestCase):
  """Convenient wrapper for tfx test cases."""

  def setUp(self):
    super().setUp()
    self.tmp_dir = os.path.join(
        os.environ.get('TEST_UNDECLARED_OUTPUTS_DIR', self.get_temp_dir()),
        self._testMethodName)
    fileio.makedirs(self.tmp_dir)
    # TODO(b/176196624): Delete following block when we drop support for TF<2.4.
    # Manually set up exit_stack because absltest.TestCase.setUp() is not called
    # in TF<2.4.
    if self._exit_stack is None:
      self._exit_stack = contextlib.ExitStack()
      self.addCleanup(self._exit_stack.close)

  def load_proto_from_text(self, path: str,
                           proto_message: message.Message) -> message.Message:
    """Loads proto message from serialized text."""
    return io_utils.parse_pbtxt_file(path, proto_message)

  def assertProtoPartiallyEquals(
      self,
      expected: Union[str, message.Message],
      actual: message.Message,
      ignored_fields: Optional[Iterable[str]] = None,
  ):
    """Asserts proto messages are equal except the ignored fields."""
    if isinstance(expected, str):
      expected = text_format.Parse(expected, actual.__class__())
      actual = copy.deepcopy(actual)
    else:
      expected = copy.deepcopy(expected)
      actual = copy.deepcopy(actual)

    # Currently only supports one-level for ignored fields.
    for ignored_field in ignored_fields or []:
      expected.ClearField(ignored_field)
      actual.ClearField(ignored_field)

    return self.assertProtoEquals(expected, actual)

  def assertArtifactMapsEqual(
      self,
      expected_artifact_map: Mapping[str, Sequence[types.Artifact]],
      actual_artifact_map: Mapping[str, Sequence[types.Artifact]],
  ) -> None:
    """Asserts that two Artifact maps are equal."""
    # The Artifact class doesn't implement __eq__ but the class is Jsonable, so
    # assert equivalence with JSON-ified versions of the maps.
    self.assertEqual(
        json_utils.dumps(expected_artifact_map),
        json_utils.dumps(actual_artifact_map))


@contextlib.contextmanager
def change_working_dir(working_dir: str):
  """Changes working directory to a given temporary directory.

  Example:
    with test_case_utils.change_working_dir(tmp_dir):

    or

    self.enter_context(test_case_utils.change_working_dir(self.tmp_dir))

  Args:
    working_dir: The new working directory. This directoy should already exist.

  Yields:
    Old working directory.
  """

  old_dir = os.getcwd()
  os.chdir(working_dir)

  yield old_dir

  os.chdir(old_dir)


class MlmdMixins:
  """Populates a mock MLMD database with Contexts, Artifacts and Excutions."""
  mlmd_handle: metadata.Metadata
  _context_type_ids: Dict[str, int]
  _artifact_type_ids: Dict[str, int]
  _execution_type_ids: Dict[str, int]

  def init_mlmd(self):
    """Initialize fake MLMD connection for testing."""
    self.mlmd_cm = mlmd_cm.MLMDConnectionManager.fake()
    self.__exit_stack = contextlib.ExitStack()
    self.__exit_stack.enter_context(self.mlmd_cm)
    assert isinstance(self, unittest.TestCase), (
        'MlmdMixins should be used along with TestCase.')
    cast(unittest.TestCase, self).addCleanup(self.__exit_stack.close)
    self._context_type_ids = {}
    self._artifact_type_ids = {}
    self._execution_type_ids = {}

  @property
  def mlmd_handle(self) -> metadata.Metadata:  # pytype: disable=annotation-type-mismatch
    return self.mlmd_cm.primary_mlmd_handle

  @property
  def store(self):
    return self.mlmd_handle.store

  def put_context_type(
      self, type_name: str,
      properties: Optional[Dict[str, metadata_store_pb2.PropertyType]] = None,
  ) -> int:
    """Puts a ContextType in the MLMD database."""
    properties = properties if properties is not None else {}
    context_type = metadata_store_pb2.ContextType(name=type_name)
    if properties is not None:
      context_type.properties.update(properties)
    result = self.store.put_context_type(context_type)
    self._context_type_ids[type_name] = result
    return result

  def _get_context_type_id(self, type_name: str):
    if type_name not in self._context_type_ids:
      self.put_context_type(type_name)
    return self._context_type_ids[type_name]

  def put_context(
      self, context_type: str, context_name: str,
      properties: Optional[Dict[str, metadata_store_pb2.PropertyType]] = None,
  ) -> metadata_store_pb2.Context:
    """Put a Context in the MLMD database."""
    context = metadata_store_pb2.Context(
        type_id=self._get_context_type_id(context_type),
        name=context_name,
        properties=data_types_utils.build_metadata_value_dict(properties))
    context_id = self.store.put_contexts([context])[0]
    return self.store.get_contexts_by_id([context_id])[0]

  def put_artifact_type(
      self, type_name: str,
      base_type: Optional[metadata_store_pb2.ArtifactType.SystemDefinedBaseType]
      = None,
      properties: Optional[Dict[str, metadata_store_pb2.PropertyType]] = None,
  ) -> int:
    """Puts an ArtifactType to the MLMD database."""
    properties = properties if properties is not None else {}
    artifact_type = metadata_store_pb2.ArtifactType(name=type_name)
    if base_type is not None:
      artifact_type.base_type = base_type
    if properties is not None:
      artifact_type.properties.update(properties)
    result = self.store.put_artifact_type(artifact_type)
    self._artifact_type_ids[type_name] = result
    return result

  def put_artifact(
      self,
      artifact_type: str,
      name: Optional[str] = None,
      uri: str = '/fake',
      properties: Optional[Dict[str, types.ExecPropertyTypes]] = None,
      custom_properties: Optional[Dict[str, types.ExecPropertyTypes]] = None,
  ) -> metadata_store_pb2.Artifact:
    """Put an Artifact in the MLMD database.

    Args:
      artifact_type: The artifact type. For example, "DummyArtifact".
      name: `Artifact.name`. Default not set.
      uri: `Artifact.uri`. Defaults to '/fake'.
      properties: The raw property values to insert in the Artifact. Example:
        {"span": 3, "version": 1}
      custom_properties: The raw custom property values to insert in the
        Artifact.

    Returns:
      The MLMD artifact.
    """
    if artifact_type not in self._artifact_type_ids:
      if properties is not None:
        property_types = {
            key: data_types_utils.get_metadata_value_type(value)
            for key, value in properties.items()
        }
      else:
        property_types = None
      type_id = self.put_artifact_type(
          artifact_type, properties=property_types)
    else:
      type_id = self._artifact_type_ids[artifact_type]

    artifact = metadata_store_pb2.Artifact(
        type_id=type_id,
        name=name,
        uri=uri,
        state=metadata_store_pb2.Artifact.LIVE,
        properties=data_types_utils.build_metadata_value_dict(properties),
        custom_properties=data_types_utils.build_metadata_value_dict(
            custom_properties),
    )
    artifact_id = self.store.put_artifacts([artifact])[0]
    return self.store.get_artifacts_by_id([artifact_id])[0]

  def put_execution_type(
      self, type_name: str,
      properties: Optional[Dict[str, metadata_store_pb2.PropertyType]] = None,
  ) -> int:
    """Puts a ExecutionType in the MLMD database."""
    properties = properties if properties is not None else {}
    execution_type = metadata_store_pb2.ExecutionType(name=type_name)
    if properties is not None:
      execution_type.properties.update(properties)
    result = self.store.put_execution_type(execution_type)
    self._execution_type_ids[type_name] = result
    return result

  def _get_execution_type_id(self, type_name: str):
    if type_name not in self._execution_type_ids:
      self.put_execution_type(type_name)
    return self._execution_type_ids[type_name]

  def put_execution(
      self,
      execution_type: str,
      properties: Optional[Dict[str, metadata_store_pb2.PropertyType]] = None,
      inputs: Optional[_ArtifactMultiMap] = None,
      outputs: Optional[_ArtifactMultiMap] = None,
      contexts: Sequence[metadata_store_pb2.Context] = (),
      name: Optional[str] = None,
      input_event_type=metadata_store_pb2.Event.INPUT,
      output_event_type=metadata_store_pb2.Event.OUTPUT,
  ) -> metadata_store_pb2.Execution:
    """Put an Execution in the MLMD database."""
    inputs = inputs if inputs is not None else {}
    outputs = outputs if outputs is not None else {}
    execution = metadata_store_pb2.Execution(
        type_id=self._get_execution_type_id(type_name=execution_type),
        name=name,
        last_known_state=metadata_store_pb2.Execution.COMPLETE,
        properties=data_types_utils.build_metadata_value_dict(properties),
    )
    artifact_and_events = []
    for input_key, artifacts in inputs.items():
      for i, artifact in enumerate(artifacts):
        event = event_lib.generate_event(input_event_type, input_key, i)
        artifact_and_events.append((artifact, event))
    for output_key, artifacts in outputs.items():
      for i, artifact in enumerate(artifacts):
        event = event_lib.generate_event(output_event_type, output_key, i)
        artifact_and_events.append((artifact, event))
    execution_id = self.store.put_execution(
        execution, artifact_and_events, contexts)[0]
    return self.store.get_executions_by_id([execution_id])[0]
