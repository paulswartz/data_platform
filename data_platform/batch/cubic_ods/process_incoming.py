
import os
import json
import boto3
import logging
import time

from data_platform.db import dbSession
from data_platform.db.models.cubic_ods_table import CubicODSTable
from data_platform.db.models.cubic_ods_load import CubicODSLoad


def run():
  # intialize boto clients
  s3 = boto3.client('s3')

  # get all load objects in incoming bucket that are from cubic ods
  loads = []
  try:
    # note: if there is NextContinuationToken, we will ignore it, as this script already runs in a loop
    response = s3.list_objects_v2(
      Bucket=os.environ.get('S3_BUCKET_INCOMING'),
      Prefix='{}cubic_ods/'.format(os.environ.get('S3_PREFIX_INCOMING', ''))
    )

    for content in response.get('Contents', []):
      logging.error(content)
      loads.append({
        'key': content.get('Key', ''),
        'size': content.get('Size', ''),
        'last_modified': content.get('LastModified', ''),
      })
  except s3.exceptions.NoSuchBucket as e:
    logging.error('[data_platform] [batch] [cubic_ods] [process_incoming]: {}'.format(e))
    raise # raise exception

  # start a db session to use with updating database
  with dbSession() as db:
    # get all tables that are not deleted
    tableRecs = db.query(CubicODSTable).filter(CubicODSTable.deleted is not None).all()
    if not tableRecs:
      return

    # loop through all the load objects
    for load in loads:

      # @todo skip any loads that are already in the database

      # determine which table the load is for
      isTableAvailable = False # for keeping track of if we find the table
      # determine is the load is CDC (Change Data Capture)
      isCDCLoad = False
      for tableRec in tableRecs:
        # check if it's regular load file
        if load.get('key', '').startswith('{}{}/'.format(os.environ.get('S3_PREFIX_INCOMING', ''), tableRec.s3_prefix)):
          isTableAvailable = True

        # check if it's a cdc load file (prefix ends with '__ct')
        if load.get('key', '').startswith('{}{}__ct/'.format(os.environ.get('S3_PREFIX_INCOMING'), tableRec.s3_prefix)):
          isTableAvailable = True
          isCDCLoad = True

        # if we have found a table record that we can associate the load with, then add the load record
        if isTableAvailable:
          # if we are trying to insert a load that matches the snapshot key, we should update the snapshot
          # value for the table
          if load.get('key', '') == '{}{}'.format(os.environ.get('S3_PREFIX_INCOMING'), tableRec.snapshot_s3_key):
            tableRec.snapshot = load.get('last_modified')

          # insert a load record
          loadRec = CubicODSLoad(**{
            'table_id': tableRec.id,
            'status': 'ready',
            'snapshot': tableRec.snapshot,
            'is_cdc': isCDCLoad,
            's3_key': load.get('key', ''),
            's3_size': load.get('size', 0),
            's3_modified': load.get('last_modified'),
          })
          db.add(loadRec)

          # commit load record insert, and any update to the table snapshot
          db.commit()

          # we found the table and inserted the record, so stop
          break

      # if we didn't find the table, let others know by logging an error
      if not isTableAvailable:
        logging.error('[data_platform] [batch] [cubic_ods] [process_incoming]: {}'.format(
          'Cubic ODS table doesn\'t exist for the load object: {}'.format(load.get('key', ''))
        ))


if __name__ == '__main__':
  run()
