#!/usr/bin/python
# Copyright (c) 2017 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type


import datetime
import re

try:
    from botocore.exceptions import ClientError, BotoCoreError
except ImportError:
    pass  # Handled by AnsibleAWSModule

from ansible.module_utils.common.dict_transformations import camel_dict_to_snake_dict
from ansible_collections.amazon.aws.plugins.module_utils.core import AnsibleAWSModule, is_boto3_error_code


DOCUMENTATION = r'''
---
module: aws_emr_cluster_info
version_added: 2.11.0
short_description: Gather information about Elastic Map Reduce Clusters in AWS.
description:
    - Gather information about Elastic Map Reduce Clusters in AWS.
author:
    - Wellington Ramos (@wmaramos)
requirements: ["boto3", "botocore"]
options:
    name:
        description:
            - Regexp used by matched the EMR Cluster Name.
        required: False
        type: str
    filters:
        description:
            - A dict of filters to apply. Each dict item consists of a filter key and value. See
              U(https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Paginator.ListClusters) for possible filters.
        required: False
        type: dict
    sort_order:
        description:
            - Define results order.
        required: False
        type: str
        default: descending
        choices: ["ascending", "descending"]
extends_documentation_fragment:
    - amazon.aws.aws
    - amazon.aws.ec2
'''

EXAMPLES = r'''
# Note: These examples do not set authentication details, see the AWS Guide for details.

- name: Gather information about all EMR Clusters
  community.aws.aws_emr_cluster_info:

- name: Gather information EMR Cluster based on name
  community.aws.aws_emr_cluster_info:
    name: "^My Cluster"

- name: Gather information about EMR Clusters based on name with RUNNI©NG state
  community.aws.aws_emr_cluster_info:
    name: "^My Cluster"
    filters:
      ClusterStates: ["RUNNING"]
'''

RETURN = r'''
clusters:
    description: A list of EMR Clusters
    returned: always
    type: complex
    contains:
        applications:
            description: the applications installed on Cluster.
            returned: always
            type: complex
            contains:
                name:
                    description: The name of the application.
                    returned: always
                    type: str
                    sample: Hadoop
                version:
                    description: The version of the application.
                    returned: always
                    type: str
                    sample: 3.2.1
                args:
                    description: Arguments passed to the application.
                    returned: when configured
                    type: list
                    elements: str
                additional_info:
                    description: Meta information about third-party applications that third-party vendors use for testing purposes.
                    returned: when configured
                    type: dict
                    elements: str
        auto_terminate:
            description: Defines if the cluster should be terminated after completiong all steps.
            returned: always
            type: bool
            sample: false
        cluster_arn:
            description: The Amazon Resource Name of the cluster.
            returned: always
            type: str
            sample: "arn:aws:elasticmapreduce:us-east-1:493456159661:cluster/j-28IVMM1I974L7"
        configurations:
            description: List of custom configurations supplied to applications bundled with Amazon EMR.
            returned: when configured
            type: complex
            contains:
                classification:
                    description: The classification within a configuration.
                    returned: when configured
                    type: str
                configurations:
                    description: A list of additional configurations to apply.
                    returned: when configured
                    type: list
                    elements: str
                properties:
                    description: A set of properties specified.
                    returned: when configured
                    type: dict
                    elements: str
        ec2_instance_attributes:
            description: Provides information about EC2 instances in the cluster.
            returned: always
            type: complex
            contains:
                ec2_availability_zone:
                    description: The availability zone of the cluster.
                    returned: always
                    type: str
                ec2_key_name:
                    description: The name of the Amazon EC2 key pair.
                    returned: when configured
                    type: str
                ec2_subnet_id:
                    description: The Amazon VPC subnet id of the cluster.
                    returned: always
                    type: str
                emr_managed_master_security_group:
                    description: The Amazon EC2 security group id for the master node.
                    returned: always
                    type: str
                emr_managed_slave_security_group:
                    description: The Amazon EC2 security group id for the core and task nodes.
                    returned: always
                    type: str
                requested_ec2_availability_zones:
                    description: One or more availability zones used with instances fleets option.
                    returned: when instances fleets is setup.
                    type: list
                    elements: str
                requested_ec2_subnet_ids:
                    description: One or more subnets used with instances fleets option.
                    returned: when instances fleets is setup.
                    type: list
                    elements: str
        id:
            description: The unique identifier for the cluster.
            returned: always
            type: str
            sample: j-28IVMM1I974L7
        instance_collection_type:
            description: The instance group configuration of the cluster.
            returned: always
            type: str
            sample: INSTANCE_GROUP
        kerberos_attributes:
            description: Kerberos attributes when Kerberos athentication is enabled using a security configuration.
            returned: when configured
            type: complex
            contains:
                realm:
                    description: The name of the Kerberos realm.
                    returned: when configured
                    type: str
                kdc_admin_password:
                    description: The password used within the cluster for kadmin service.
                    returned: when configured
                    type: str
                cross_realm_trust_principal_password:
                    description: The cross-realm principal password.
                    returned: when configured
                    type: str
                ad_domain_join_user:
                    description: The user used to join resources in cross-realm.
                    returned: when configured
                    type: str
                ad_domain_join_password:
                    description: The password used to join resources in cross-realm.
                    returned: when configured
                    type: str
        log_uri:
            description: The path to Amazon S3 location where logs  are stored.
            returned: when configured
            type: str
        master_public_dns_name:
            description: The DNS record point to master node.
            returned: always
            type: str
        name:
            description: Friendly name of the cluster.
            returned: always
            type: str
        normalized_instance_hours:
            description: An approximation of the cost of the cluster, represented in m1.small/hours.
            returned: always
            type: int
        placement_groups:
            description: The configurations to placement strategy applied to instance roles during clustter creation.
            returned: when configured
            type: complex
            contains:
                instance_role:
                    description: Role of the cluster.
                    returned: when configured
                    type: str
                placement_strategy:
                    description: EC2 placement strategy associated.
                    returned: when configured
                    type: str
        release_label:
            description: The EMR release label, wich determines the version of the applications.
            returned: always
            type: str
            sample: emr-6.2.0
        scale_down_behavior:
            description: The way that individual EC2 instance terminate when automatic scale-in.
            returned:  when configured
            type: str
            sample: TERMINATE_AT_TASK_COMPLETION
        service_role:
            description: The IAM role that will be assumed by EMR Service.
            returned: always
            type: str
            sample: EMR_DefaultRole
        status:
            description: The current status of the cluster.
            returned: always
            type: str
            sample: RUNNING
        step_concurrency_level:
            description: The number of steps that can be executed concurrently.
            returned: when configured
            type: str
        tags:
            description: EC2 tags of the cluster.
            returned: when configured
            type: complex
            contains:
                key:
                    description: The name of key.
                    returned: when configured
                    type: str
                value:
                    description: The value of the key.
                    returned: when configured
                    type: str
        termination_protected:
            description: Defines if termination protection is enabled.
            returned: always
            type: bool
        visible_to_all_users:
            description: Defines visibility of the custer to all users.
            returned: always
            type: bool
'''


def main():
    argument_spec = dict(
        name=dict(type='str'),
        filters=dict(type='dict', default={}),
        sort_order=dict(type='str', default='descending', choices=[
                        'ascending', 'descending'])
    )

    module = AnsibleAWSModule(
        supports_check_mode=True,
        argument_spec=argument_spec
    )

    try:
        client = module.client('emr')
    except (ClientError, BotoCoreError) as e:
        module.fail_json_aws(e)

    filters = module.params.get('filters')

    if 'CreatedAfter' in filters:
        filters.update({'CreatedAfter': datetime.strtime(
            filters['CreatedAfter'], '%Y-%m-%d')})

    if 'CreatedBefore' in filters:
        filters.update({'CreatedBefore': datetime.strtime(
            filters['CreatedBefore'], '%Y-%m-%d')})

    try:
        paginator = client.get_paginator('list_clusters')
        response = paginator.paginate(
            **filters).build_full_result()['Clusters']
    except (ClientError, BotoCoreError) as e:
        module.fail_json_aws(e)

    cluster_name = module.params.get('name')

    if cluster_name:
        response = [emr for emr in response if re.compile(
            cluster_name).match(emr['Name'])]

    try:
        emr_clusters = [client.describe_cluster(ClusterId=emr['Id'])[
            'Cluster'] for emr in response]
    except (ClientError, BotoCoreError) as e:
        module.fail_json_aws(e)

    sort_order = module.params.get('sort_order')

    emr_clusters.sort(key=lambda e: e['Status']['Timeline']
                      ['CreationDateTime'], reverse=(sort_order == 'descending'))

    snaked_clusters = camel_dict_to_snake_dict({'clusters': emr_clusters})

    module.exit_json(**snaked_clusters)


if __name__ == '__main__':
    main()
