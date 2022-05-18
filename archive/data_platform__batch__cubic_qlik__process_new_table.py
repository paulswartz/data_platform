
import os
import argparse
import boto3
from dotenv import load_dotenv

import data_platform.db as db
from data_platform.db.models import cubic_qlik_table, cubic_qlik_batch_load, cubic_qlik_cdc_load


# get enviroment variables from .env file, if it exists (mainly local development)
load_dotenv()

# initialize clients (this uses the default session, i.e. instance profile/role)
s3 = boto3.client('s3')
glue = boto3.client('glue')

# if we are on local, then make some updates to the clients
if os.environ.get('ENV') == 'local':
  # and if we have specified a boto profile to use, then override clients to use the specific session
  if os.environ.get('BOTO_PROFILE'):
    botoSession = boto3.Session(profile_name=os.environ.get('BOTO_PROFILE'))
    s3 = botoSession.client('s3')

# main function to run
def main(tableName=None, dryRun=True):
  # instatiate models
  CubicQlikTable = cubic_qlik_table.CubicQlikTable
  CubicQlikBatchLoad = cubic_qlik_batch_load.CubicQlikBatchLoad
  CubicQlikCDCLoad = cubic_qlik_cdc_load.CubicQlikCDCLoad

  # verify table is in our data platform
  tableRec = None
  with db.session() as session:
    tableRec = session.query(CubicQlikTable).filter_by(name=tableName).first()
    #  if no table found, then log and return
    if not tableRec:
      print('Table Not Found: {}'.format(tableName))
      return

  # get all current objects available for import from s3 for the table
  # note: there could be hundreds or thousands of objects, so we paginate
  batchLoadObjectKeys = []
  cdcLoadObjectKeys = []
  paginator = s3.get_paginator('list_objects_v2')
  paginatorParameters = {
    'Bucket': os.environ.get('S3_BUCKET_INCOMING'),
    'Prefix': 'cubic-ods-qlik-ingest/'
  }
  # add addional prefix if we have one set in our environment (usually on local)
  if os.environ.get('S3_PREFIX_INCOMING'):
    paginatorParameters['Prefix'] = os.environ.get('S3_PREFIX_INCOMING') + paginatorParameters['Prefix']
  # loop through pages of list
  for page in paginator.paginate(**paginatorParameters):
    for obj in page.get('Contents', []):
      key = obj.get('Key', '')

      # if it 'ends' with '__ct' it's a cdc load object
      if key.startswith('{}{}__ct/'.format(os.environ.get('S3_PREFIX_INCOMING', ''), tableRec.name)):
        cdcLoadObjectKeys.append(key)
      elif key.startswith('{}{}/'.format(os.environ.get('S3_PREFIX_INCOMING', ''), tableRec.name)):
        batchLoadObjectKeys.append(key)

  # get all batch and cdc load records for the table, and dump their s3 keys
  batchLoadRecS3Keys = []
  cdcLoadRecS3Keys = []
  with db.session() as session:
    # batch
    batchLoadRecs = session.query(CubicQlikBatchLoad).filter_by(table_id=tableRec.id).all()
    for loadRec in batchLoadRecs:
      batchLoadRecS3Keys.append(loadRec.s3_key)
    # cdc
    cdcLoadRecs = session.query(CubicQlikCDCLoad).filter_by(table_id=tableRec.id).all()
    for loadRec in cdcLoadRecs:
      cdcLoadRecS3Keys.append(loadRec.s3_key)

  # create list of object keys that we need to run jobs for
  # batch
  batchJobKeys = []
  for objectKey in batchLoadObjectKeys: # @todo optimize (maybe https://datascienceparichay.com/article/set-difference-python/)
    if objectKey not in batchLoadRecS3Keys:
      batchJobKeys.append(objectKey)
  # cdc
  cdcJobKeys = []
  for objectKey in cdcLoadObjectKeys: # @todo optimize
    if objectKey not in cdcLoadRecS3Keys:
      cdcJobKeys.append(objectKey)

  # loop over jobs that are left and run them
  # batch
  for objectKey in batchJobKeys + cdcJobKeys:
    if dryRun:
      print('Run "docker-compose run --rm glue__local /glue/bin/gluesparksubmit /data_platform/aws/s3/glue_jobs/cubic_qlik__ingest_load.py --JOB_NAME cubic_qlik__ingest_load --OBJECT_KEY {}"'.format(objectKey))
    else:
      glue.start_job_run(
        JobName='dataplatform-{}-cubic-qlik-ingest-load'.format(os.environ.get('ENV')),
        Arguments={
          'OBJECT_KEY': objectKey
        }
      )
      # @todo make sure the job ran with 'glue.get_job_run'

# script controller
if __name__ == '__main__':

  dryRun = False
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--run',
    action='store_const',
    const=True,
    default=False
  )
  parser.add_argument(
    '--table',
    required=True
  )
  args = parser.parse_args()

  main(tableName=args.table, dryRun=not args.run)