import boto3
import pandas as pd
import logging
import secrets
import botocore
import json
rsd = boto3.client('redshift-data', region_name='us-east-1')
secretsmanager = boto3.client('secretsmanager', region_name='us-east-1')
APP_NAME = 'rassi'
AWSACCTID = 'ACCTID'
REDSHIFT_USER_ROLE_NAME = 'ROLE_NAME'
REDSHIFT_USER_PERMISSIONS_LIST = list(''.split(" "))
REDSHIFT_SERVICE_ACCOUNT_ADMIN_LIST = list(''.split(" "))
REDSHIFT_ADMIN_LIST = list(''.split(" "))
def secretsInit(secretInfo):
    clusterPrefix = secretInfo['clusterEndpoint'].split('.')[0]
    secret = {
        "username": secretInfo['username'],
        "password": secretInfo['tempPassword'],
        "dbname": secretInfo['dbname'],
        "engine": "redshift",
        "host": secretInfo['clusterEndpoint'],
        "port": 5439,
        "masterarn": secretInfo['masterarn']
    }
    # Check if secret exists
    try:
        secretCheck = secretsmanager.describe_secret(
            SecretId='{}/{}/{}/{}'.format(APP_NAME, clusterPrefix, secretInfo['dbname'], secretInfo['username'])
        )
    # Catch the boto3 error if the secret does not exist
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # try to create the secret and rotate
            try:
                # DO ALTER SECRET HERE
                print('[SQL] ALTER USER \"{}\" password *********'.format(secretInfo['username']))
                query("alter user \"{}\" password \'{}\';".format(secretInfo['username'],secretInfo['tempPassword']))
                createSecretResponse = secretsmanager.create_secret(
                    Name='{}/{}/{}/{}'.format(APP_NAME, clusterPrefix, secretInfo['dbname'], secretInfo['username']),
                    SecretString=json.dumps(secret)
                    # TODO ADD ENCRYPTION IN
                    #KmsKeyId=SECRETS_MANAGER_KMS_ARN
                )
                putResourcePolicyResponse = secretsmanager.put_resource_policy(
                    SecretId='{}/{}/{}/{}'.format(APP_NAME, clusterPrefix, secretInfo['dbname'], secretInfo['username']),
                    ResourcePolicy=json.dumps(buildSecretResourcePolicy(secretInfo['username']))
                )
                if secretInfo['username'].startswith('s-tbdp'):
                    rotateSecretResponse = secretsmanager.rotate_secret(
                        SecretId='{}/{}/{}/{}'.format(APP_NAME, clusterPrefix, secretInfo['dbname'],
                                                    secretInfo['username']),
                        RotationLambdaARN=secretInfo['passwordRotationLambdaArn'],
                        RotationRules={
                            'AutomaticallyAfterDays': 365
                        }
                    )
                else:
                    rotateSecretResponse = secretsmanager.rotate_secret(
                        SecretId='{}/{}/{}/{}'.format(APP_NAME, clusterPrefix, secretInfo['dbname'],
                                                    secretInfo['username']),
                        RotationLambdaARN=secretInfo['passwordRotationLambdaArn'],
                        RotationRules={
                            'AutomaticallyAfterDays': 90
                        }
                    )
                
            except botocore.exceptions.ClientError as err:
                print('[ERROR] {}'.format(err))
        else:
            print("Error initializing secret for {}".format(secretInfo['username']))
            print('[ERROR] {}'.format(e))

def buildSecretResourcePolicy(username):
    try:
        with open('secret_resource_policy_template.json') as secretResourcePolicyTemplateJSON:
            secretResourcePolicyTemplate = json.loads(secretResourcePolicyTemplateJSON.read())
            for statement in secretResourcePolicyTemplate['Statement']:
                if 'userPermissions' == statement['Sid'] and not username.startswith('s-tbdp'):
                    statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(
                        'arn:aws:sts::{}:assumed-role/{}/{}'.format(
                            AWSACCTID,
                            REDSHIFT_USER_ROLE_NAME,
                            username
                        )
                    )
                    statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append('arn:aws:iam::{}:role/{}'.format(
                        AWSACCTID,
                        REDSHIFT_USER_ROLE_NAME
                    )
                    )
                    statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(
                        'arn:aws:iam::{}:root'.format(
                            AWSACCTID
                        )
                    )
                    for lambdaArn in REDSHIFT_USER_PERMISSIONS_LIST:
                        statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(lambdaArn)
                elif 'userPermissions' == statement['Sid'] and username.startswith('s-tbdp'):
                    statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(
                        'arn:aws:iam::{}:root'.format(
                            AWSACCTID
                        )
                    )
                    for redshiftSAAdmin in REDSHIFT_SERVICE_ACCOUNT_ADMIN_LIST:
                        statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(redshiftSAAdmin)
                    for lambdaArn in REDSHIFT_USER_PERMISSIONS_LIST:
                        statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(lambdaArn)
                elif 'adminPermissions' == statement['Sid']:
                    statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append('arn:aws:iam::{}:role/{}'.format(
                        AWSACCTID,
                        REDSHIFT_USER_ROLE_NAME
                    )
                    )
                    statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(
                        'arn:aws:sts::{}:assumed-role/{}/{}'.format(
                            AWSACCTID,
                            REDSHIFT_USER_ROLE_NAME,
                            username
                        )
                    )
                    statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(
                        'arn:aws:iam::{}:root'.format(
                            AWSACCTID
                        )
                    )
                    # Add any admin role users to be able to describe the secret
                    for redshiftAdmin in REDSHIFT_ADMIN_LIST:
                        statement['Condition']['StringNotEquals']['aws:PrincipalArn'].append(redshiftAdmin)
            return secretResourcePolicyTemplate
    except botocore.exceptions.ClientError as ce:
        print("Error building secret resource policy for {}".format(username))
        print('[ERROR] {}'.format(ce))
    except Exception as e:
        print('[ERROR] {}'.format(e))
def post_process(meta, records):
    columns = [k["name"] for k in meta]
    rows = []
    for r in records:
        cluster="redshift-cluster-1", 
        tmp = []
        for c in r:
            tmp.append(c[list(c.keys())[0]])
        rows.append(tmp)
    return pd.DataFrame(rows, columns=columns)

def query(
        sql, 
        dbuser="redshift_data_api_user",
        database="dev",
        cluster="redshift-cluster-1"
    ):
    resp = rsd.execute_statement(
        Database=database,
        ClusterIdentifier=cluster,
        DbUser=dbuser,
        Sql=sql
    )
    qid = resp["Id"]
    print('Query ID: {}'.format(qid))
    desc = None
    while True:
        desc = rsd.describe_statement(Id=qid)
        if desc["Status"] == "FINISHED":
            break
            print(desc["ResultRows"])
    if desc and desc["ResultRows"]  > 0:
        result = rsd.get_statement_result(Id=qid)
        rows, meta = result["Records"], result["ColumnMetadata"]
        return post_process(meta, rows)

def provisionSecretsForExistingUsers(row):
    tmpPassword = secrets.token_urlsafe(32)
    user = row['usename']
    print('[INFO] User {} will be added.'.format(user))
    with open('config.json') as confJSON:
        conf = json.loads(confJSON.read())
        secretInfo = {
            'username': user,
            'tempPassword': tmpPassword,
            'dbname': conf['dbName'],
            'clusterEndpoint': conf['clusterEndpoint'],
            'passwordRotationLambdaArn': conf['passwordRotationLambdaArn'],
            'masterarn': conf['masterArn']
        }
        secretsInit(secretInfo)
def main():
    users = query("select * from pg_user;")
    users.to_csv('users.csv')

    groups = query("select * from pg_group;")
    groups.to_csv('groups.csv')

    groupGrantDDLs = query("select ddl from security_audit.v_generate_user_grant_revoke_ddl where ddltype='grant';")
    groupGrantDDLs.to_csv('groupGrantDDLs.csv')

    users.apply(provisionSecretsForExistingUsers, axis=1)


main()
