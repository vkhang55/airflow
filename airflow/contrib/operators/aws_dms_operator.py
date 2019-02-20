# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys

from math import pow
from time import sleep

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html?highlight=database%20migration#DatabaseMigrationService.Client
class AWSDMSOperator(BaseOperator):
    """
    Execute a task on AWS Database Migration System (DMS)
    """

    ui_color = '#c3dae0'
    client = None
    arn = None
    template_fields = ('job_name', 'overrides',)

    @apply_defaults
    def __init__(self, task_name, job_definition, job_queue, overrides, max_retries=4200,
        aws_conn_id=None, region_name=None, **kwargs):
      super(AWSDMSOperator, self).__init__(**kwargs)

    def execute(self, context):
      self.log.info(
          'Running AWS DMS task',
          self.task_name
          )
      self.log.info('AWSBatchOperator overrides: %s', self.overrides)

      self.client = self.hook.get_client_type(
          'dms',
          region_name=self.region_name
        )

      # Code
      client = boto3.client('dms')

    def _wait_for_task_ended(self):
      # Code

    def _check_success_task(self):
      # Code

    def get_hook(self):
      # Code

    def on_kill(self):
      # Code
