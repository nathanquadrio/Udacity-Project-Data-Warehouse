import json
import boto3            # type: ignore
import configparser
import pandas as pd     # type: ignore
import time


class AWSClients:
    def __init__(self, key, secret, region="us-west-2"):
        self.key = key
        self.secret = secret
        self.region = region
        self._ec2 = None
        self._s3 = None
        self._iam = None
        self._redshift = None

    @property
    def ec2(self):
        if self._ec2 is None:
            self._ec2 = boto3.resource('ec2',
                                       region_name=self.region,
                                       aws_access_key_id=self.key,
                                       aws_secret_access_key=self.secret)
        return self._ec2

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.resource('s3',
                                      region_name=self.region,
                                      aws_access_key_id=self.key,
                                      aws_secret_access_key=self.secret)
        return self._s3

    @property
    def iam(self):
        if self._iam is None:
            self._iam = boto3.client('iam',
                                     aws_access_key_id=self.key,
                                     aws_secret_access_key=self.secret,
                                     region_name=self.region)
        return self._iam

    @property
    def redshift(self):
        if self._redshift is None:
            self._redshift = boto3.client('redshift',
                                          region_name=self.region,
                                          aws_access_key_id=self.key,
                                          aws_secret_access_key=self.secret)
        return self._redshift
    
def read_dwh_config(file_path):
    config = configparser.ConfigParser()
    config.read_file(open(file_path))

    data = {
        "Param": ["KEY", "SECRET", "DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
        "Value": [
            config.get('AWS', 'KEY'),
            config.get('AWS', 'SECRET'),
            config.get("DWH", "DWH_CLUSTER_TYPE"),
            config.get("DWH", "DWH_NUM_NODES"),
            config.get("DWH", "DWH_NODE_TYPE"),
            config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
            config.get("DWH", "DWH_DB"),
            config.get("DWH", "DWH_DB_USER"),
            config.get("DWH", "DWH_DB_PASSWORD"),
            config.get("DWH", "DWH_PORT"),
            config.get("DWH", "DWH_IAM_ROLE_NAME")
        ]
    }

    return pd.DataFrame(data)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)  # Use None instead of -1
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def cleanup_resources(redshift, iam, cluster_identifier, iam_role_name):
    try:
        print("Deleting the Redshift cluster...")
        redshift.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)
        
        # Wait for the cluster to be deleted
        while True:
            try:
                myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
                df = prettyRedshiftProps(myClusterProps)
                print(df)
                print("Waiting for the cluster to be deleted...")
                time.sleep(30)
            except redshift.exceptions.ClusterNotFoundFault:
                print("Cluster deleted successfully.")
                break

        print("Detaching policy from IAM role...")
        iam.detach_role_policy(RoleName=iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print("Deleting the IAM role...")
        iam.delete_role(RoleName=iam_role_name)
        print("IAM role deleted successfully.")
        
    except Exception as e:
        print(f"Error cleaning up resources: {e}")

def create_iam_role(iam, role_name):
    try:
        #print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                    'Version': '2012-10-17'
                }
            )
        )    
    except Exception as e:
        print(e)

    #print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    #print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=role_name)['Role']['Arn']

    #print(roleArn)
    return roleArn