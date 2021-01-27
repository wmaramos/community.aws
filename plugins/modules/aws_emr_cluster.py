#!/usr/bin/python
# Copyright (c) 2017 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import re
import json

try:
    from botocore.exceptions import ClientError, BotoCoreError, WaiterError
except ImportError:
    pass  # Handled by AnsibleAWSModule

from ansible.module_utils.common.dict_transformations import camel_dict_to_snake_dict, snake_dict_to_camel_dict

from ansible_collections.amazon.aws.plugins.module_utils.core import AnsibleAWSModule

DOCUMENTATION = r'''
'''

EXAMPLES = r'''
'''

RETURN = r'''
'''


def _gather_cluster_info(client, module, filters={}):
    cluster_name = module.params.get('name')

    try:
        paginator = client.get_paginator('list_clusters')
        response = paginator.paginate(
            **filters).build_full_result()['Clusters']
    except (ClientError, BotoCoreError) as e:
        module.fail_json_aws(e)

    response = [emr for emr in response if re.compile(
        cluster_name).match(emr['Name'])]

    if response:
        if len(response) > 1:
            module.warn(
                warning='More than one Cluster match with name {cluster_name} just the first one will be used')

        try:
            response = client.describe_cluster(
                ClusterId=response[0]['Id'])['Cluster']
        except (ClientError, BotoCoreError) as e:
            module.fail_json_aws(e)

    return response


def _create_cluster(client, module):
    return []


def _terminate_cluster(client, module, cluster_id):
    wait = module.params.get('wait')

    try:
        client.terminate_job_flows(JobFlowsIds=[cluster_id])
    except (ClientError, BotoCoreError) as e:
        module.fail_json_aws(e)

    if wait:
        waiter = client.get_waiter('cluster_terminated')
        wait_delay = module.params.get('wait_delay')
        wait_max_attempts = module.params.get('wait_max_attempts')

        try:
            waiter.wait(ClusterId=cluster_id, WaiterConfig={
                        'Delay': wait_delay, 'MaxAttempts': wait_max_attempts})
        except (WaiterError, BotoCoreError) as e:
            module.fail_json_aws(e)

    try:
        response = client.describe_cluster(ClusterId=cluster_id)['Cluster']
    except (ClientError, BotoCoreError) as e:
        module.fail_json_aws(e)

    return response


def ensure_custer_present(client, module):
    instances = module.params.get('instances')

    module.exit_json(changed=False, **instances)

    filters = {'ClusterStates': ['STARTING',
                                 'BOOTSTRAPPING',
                                 'RUNNING',
                                 'WAITING']}

    response = _gather_cluster_info(client, module, filters)

    if response:
        check_status = [{'Status': {'State': state}}
                        in response for state in filters['ClusterStates']]

        if any(check_status):
            changed = False

            return changed, response

    changed = True

    response = _create_cluster(client, module)

    return changed, response


def ensure_cluster_absent(client, module):
    response = _gather_cluster_info(client, module)

    if 'Status' in response:
        check_status = [response['Status']['State'] == state
                        for state in ['STARTING',
                                      'BOOTSTRAPPING',
                                      'RUNNING',
                                      'WAITING']]

        if any(check_status):
            changed = True

            response = _terminate_cluster(client, module, response['Id'])
        else:
            changed = False
    else:
        changed = False

    return changed, response


def main():
    argument_spec = dict(
        name=dict(type='str', required=True),
        log_uri=dict(type='str'),
        additional_info=dict(type='json'),
        release_label=dict(type='str'),
        instances=dict(
            type='dict',
            options=dict(
                instance_groups=dict(type='json'),
                instance_fleets=dict(type='json'),
                ec2_key_name=dict(type='str'),
                placement=dict(type='dict'),
                keep_job_flow_alive_when_no_steps=dict(
                    type='bool', default=False),
                termination_protected=dict(type='bool', default=False),
                ec2_subnet_id=dict(type='str'),
                ec2_subnets_ids=dict(type='list', elements='str'),
                emr_managed_master_security_group=dict(type='str'),
                emr_managed_slave_security_group=dict(type='str'),
                additional_master_security_groups=dict(
                    type='list', elements='str'),
                additional_slave_security_groups=dict(
                    type='list', elements='str')
            )
        ),
        steps=dict(
            type='list',
            elements='dict',
            options=dict(
                name=dict(type='str'),
                action_on_failure=dict(
                    type='str',
                    choices=[
                        'TERMINATE_JOB_FLOW',
                        'TERMINATE_CLUSTER',
                        'CANCEL_AND_WAIT',
                        'CONTINUE'
                    ],
                    default='TERMINATE_CLUSTER'
                ),
                hadoop_jar_step=dict(type='dict')
            )
        ),
        bootstrap_actions=dict(type='list', elements='dict'),
        supported_products=dict(type='list', elements='str'),
        new_supported_products=dict(type='list', elements='dict'),
        applications=dict(type='list', elements='dict'),
        visible_to_all_users=dict(type='bool', default=True),
        job_flow_role=dict(type='str', default='EMR_EC2_DefaultRole'),
        service_role=dict(type='str', default='EMR_DefaultRole'),
        security_configuration=dict(type='str'),
        auto_scaling_role=dict(
            type='str', default='EMR_AutoScaling_DefaultRole'),
        scale_down_behavior=dict(typ='str', choices=[
                                 'TERMINATE_AT_INSTANCE_HOUR', 'TERMINATE_AT_TASK_COMPLETION']),
        custom_ami_id=dict(type='str'),
        ebs_root_volume_size=dict(type='int'),
        repo_upgrade_on_boot=dict(type='str', choices=['SECURITY', 'NONE']),
        kerberos_attributes=dict(type='dict', no_log=True),
        state=dict(type='str', choices=[
                   'present', 'absent'], default='present'),
        wait=dict(type='bool', default=False),
        wait_delay=dict(type='int', default=30),
        wait_max_attempts=dict(type='int', default=60)
    )

    module = AnsibleAWSModule(
        supports_check_mode=True,
        argument_spec=argument_spec,
        required_if=[
            ['state', 'present', ['instances', 'release_label']]
        ]
    )

    try:
        client = module.client('emr')
    except (ClientError, BotoCoreError) as e:
        module.fail_json_aws(e)

    state = module.params.get('state')

    case = {
        'present': ensure_custer_present,
        'absent': ensure_cluster_absent
    }

    changed, response = case.get(state)(client, module)

    snaked_response = camel_dict_to_snake_dict({'cluster': response})

    module.exit_json(changed=changed, **snaked_response)


if __name__ == '__main__':
    main()
