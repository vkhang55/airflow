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
    @apply_defaults
    def __init__(self, replication_task_arn, start_replication_task_type='start-replication', max_retries=4200, aws_conn_id=None, region_name=None, cdc_start_time=None, cdc_start_position=None, cdc_stop_position=None, **kwargs):
      super(AWSDMSOperator, self).__init__(**kwargs)
      self.replication_task_arn         = replication_task_arn
      self.start_replication_task_type  = start_replication_task_type
      self.max_retries                  = max_retries
      self.aws_conn_id                  = aws_conn_id 
      self.region_name                  = region_name
      self.cdc_start_time               = cdc_start_time
      self.cdc_start_position           = cdc_start_position
      self.cdc_stop_position            = cdc_stop_position 

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html?highlight=database%20migration#DatabaseMigrationService.Client.start_replication_task
    def execute(self, context):
      self.log.info(
          'Running AWS DMS task',
          self.task_name
          )

      self.client = self.hook.get_client_type(
          'dms',
          region_name=self.region_name
        )

      try:
        # StartReplicationTaskType='start-replication'|'resume-processing'|'reload-target',
        response = self.client.start_replication_task(
            ReplicationTaskArn=self.replication_task_arn,
            StartReplicationTaskType=self.start_replication_task_type,
            CdcStartTime=self.cdc_start_time,
            CdcStartPosition=self.cdc_start_position,
            CdcStopPosition=self.cdc_stop_position
            )
        
        self.log.info('AWS DMS Replication Task started: %s', response)

        replication_task_identifier = response['ReplicationTask']['ReplicationTaskIdentifier']

        self._wait_for_task_ended();
        self._check_success_task();

        self.log.info("AWS DMS Replication task {0} - ARN={1} has successfully executed".format(replication_task_identifier, self.replication_task_arn))
      except Exception as e:
        self.log.info("AWS DMS Replication task has failed execution".format(replication_task_arn))
        raise AirflowException(e)

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html?highlight=database%20migration#DatabaseMigrationService.Client.get_waiter
    def _wait_for_task_ended(self):
      # Code
      try:
        # TODO: alternatively, we can do what's done below and look for the status = 'stopped' or when it stops running
        waiter = self.client.get_waiter('replication_task_stopped')
        waiter.config.max_attempts = sys.maxsize
        waiter.wait(replication_task_arn=[self.replication_task_arn])
      except ValueError:
        retry = True
        
        # We want to wait for the task to end

    # https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Monitoring.html - task status
    def _check_success_task(self):
      # Code
      response = self.client.describe_replication_tasks(
          Filters=[
            {
              'Name': 'replication-task-arn',
              'Values': [
                self.replication_task_arn,
              ]
            },
          MaxRecords=123,
          Marker='string'
          ],
        )
      
      if len(response['ReplicationTasks']) == 0:
        raise AirflowException("Replication Task {0} couldn't be found".format(self.replication_task_arn))
      else:
        response              = response['ReplicationTasks'][0]
        status                = response['Status']
        last_error_message    = response['LastFailureMessage']
        stop_reason           = response['StopReason']

        # Example
        # 
        # 'ReplicationTaskStats': {
        #     'FullLoadProgressPercent': 123,
        #     'ElapsedTimeMillis': 123,
        #     'TablesLoaded': 123,
        #     'TablesLoading': 123,
        #     'TablesQueued': 123,
        #     'TablesErrored': 123
        #     }
        full_load_progress_percent  = response['ReplicationTaskStats']['FullLoadProgressPercent']
        elapsed_time_millis         = response['ReplicationTaskStats']['ElapsedTimeMillis']
        tables_loaded               = response['ReplicationTaskStats']['TablesLoaded']
        tables_loading              = response['ReplicationTaskStats']['TablesLoading']
        tables_queued               = response['ReplicationTaskStats']['TablesQueued']
        tables_errored              = response['ReplicationTaskStats']['TablesErrored']

        if full_load_progress_percent < 100:
          raise AirflowException("Replication Task {0} did not complete successfully. {1} tables were queued, {2} tables were loaded, {3} tables were errored, and {4} tables are loading. The last error message is: {5}".format(self.replication_task_arn, tables_queued, tables_loaded, tables_errored, tables_loading, last_error_message))
        else: # successfully completed 100%
          self.log.info("AWS DMS Replication Task ARN {0} completed in {1} milliseconds and {2} tables were loaded".format(self.replication_task_arn, elapsed_time_millis, tables_loaded))


    def get_hook(self):
      return AwsHook(
          aws_conn_id=self.aws_conn_id
      )

    def on_kill(self):
      # Code
      response = self.client.stop_replication_task(
          ReplicationTaskArn=self.replication_task_arn,
          StopReason='Killed by User via Airflow'
      )
