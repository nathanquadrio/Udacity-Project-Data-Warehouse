import pandas as pd # type: ignore
import boto3        # type: ignore
import json
import psycopg2     # type: ignore
import os
import logging
import time
import atexit
from botocore.exceptions import ClientError # type: ignore
from dhwFunctions import read_dwh_config, AWSClients, create_iam_role, prettyRedshiftProps, cleanup_resources # type: ignore

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_config_value(df, param_name):
    value = df[df['Param'] == param_name]['Value']
    if not value.empty:
        return value.values[0]
    else:
        raise ValueError(f"Parameter {param_name} not found in configuration.")

# Specify the path to your configuration file
config_file_path = 'dwh.cfg'

# Read the configuration and get the DataFrame
df = read_dwh_config(config_file_path)
#logger.info("Configuration DataFrame:\n%s", df)

# Extract data from the configuration DataFrame
try:
    KEY                     = get_config_value(df, 'KEY')
    SECRET                  = get_config_value(df, 'SECRET')
    DWH_IAM_ROLE_NAME       = get_config_value(df, 'DWH_IAM_ROLE_NAME')
    DWH_CLUSTER_TYPE        = get_config_value(df, 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES           = get_config_value(df, 'DWH_NUM_NODES')
    DWH_NODE_TYPE           = get_config_value(df, 'DWH_NODE_TYPE')
    DWH_CLUSTER_IDENTIFIER  = get_config_value(df, 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB                  = get_config_value(df, 'DWH_DB')
    DWH_DB_USER             = get_config_value(df, 'DWH_DB_USER')
    DWH_DB_PASSWORD         = get_config_value(df, 'DWH_DB_PASSWORD')
    DWH_PORT                = get_config_value(df, 'DWH_PORT')
except ValueError as e:
    logger.error(e)
    exit(1)

# Initialize AWS clients
aws_clients = AWSClients(key=KEY, secret=SECRET)

# Access the clients and resources
ec2 = aws_clients.ec2
s3 = aws_clients.s3
iam = aws_clients.iam
redshift = aws_clients.redshift


# #Check out the sample data sources on S3
# sampleDbBucket =  s3.Bucket("awssampledbuswest2")
# #Iterate over bucket objects starting with "ssbgz" and print
# for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
#     print(obj.key)

# Create IAM role and get its ARN
roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
# Example usage
print("IAM Role ARN:", roleArn)

#Create a RedShift Cluster
try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)

while True:
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = prettyRedshiftProps(myClusterProps)
        #print(df)
        if myClusterProps['ClusterStatus'] == 'available':
            print("Cluster is now available.")
            break
    except redshift.exceptions.ClusterNotFoundFault:
        print("Cluster not found, waiting for it to be created...")


DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
logger.info("DWH_ENDPOINT :: %s", DWH_ENDPOINT)
logger.info("DWH_ROLE_ARN :: %s", DWH_ROLE_ARN)

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

# Create the connection string
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
logger.info("Connection string: %s", conn_string)

# Create a connection to the Redshift database
try:
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    
    # Execute a simple query to test the connection
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"Connected to Redshift, version: {version[0]}")

    # Clean up
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error connecting to Redshift: {e}")

# # Clean up resources
# def cleanup_on_exit():
#     cleanup_resources(redshift, iam, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME)

# atexit.register(cleanup_on_exit)
