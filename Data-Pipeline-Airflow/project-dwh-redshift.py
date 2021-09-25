from time import time
import boto3
import json
import logging
import configparser


def create_redshift_cluster(config):
    """
    Create new redshift cluster database

    args:
        config: config parameters for creating redshift cluster on AWS
    """

    key = config.get('AWS', 'key')
    secret = config.get('AWS', 'secret')
    cluster_type = config.get("CLUSTER", "cluster_type")
    num_nodes = config.get("CLUSTER", "num_nodes")
    node_type = config.get("CLUSTER", "node_type")
    cluster_identifier = config.get("CLUSTER", "cluster_identifier")
    dbname = config.get("CLUSTER", "dbname")
    user = config.get("CLUSTER", "user")
    password = config.get("CLUSTER", "password")
    port = config.get("CLUSTER", "port")
    iam_role_name = config.get("CLUSTER", "iam_role_name")

    # Create IAM and Redshift client
    iam = boto3.client('iam', aws_access_key_id=key,
                       aws_secret_access_key=secret,
                       region_name='us-west-2'
                       )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=key,
                            aws_secret_access_key=secret
                            )

    # Get Role ARN
    role_arn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']

    try:
        response = redshift.create_cluster(
            # Hardware
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),

            # Identifiers & Credentials
            DBName=dbname,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=user,
            MasterUserPassword=password,

            # Roles (for s3 access)
            IamRoles=[role_arn]
        )
        return redshift, cluster_identifier
    except Exception as e:
        raise RuntimeError(f"Error occurred in creating AWS redshift cluster")


def main():
    """
    Create AWS Redshift Cluster
    Use Python SDK boto3 to create new AWS Redshift cluster
    Load AWS Redshift Parameters from configuration file
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    redshift, cluster_identifier = create_redshift_cluster(config)

    
if __name__ == "__main__":
    main()