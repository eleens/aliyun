import sys
import traceback
import pymssql
import time
from datahub import DataHub
from datahub.utils import Configer
from decimal import Decimal
from datetime import datetime
import time
from datahub.models import Topic, RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
from datahub.errors import DatahubException, ObjectAlreadyExistException


access_id = "***"
access_key = "***"
endpoint = "http://dh-cn-hangzhou.aliyuncs.com"

dh = DataHub(access_id, access_key, endpoint)
topic_name = "st_stbprp_b_topic"
project_name = 'bigdata_odps'
column_list = ['STCD', 'STNM', 'RVNM', 'HNNM', 'BSNM', 'LGTD', 'LTTD', 'STLC', 'ADDVCD','DTMNM', 'DTMEL', 'DTPR', 'STTP', 'ITEM', 'FRGRD', 'ESSTYM', 'BGFRYM', 'ATCUNIT', 'ADMAUTH', 'LOCALITY', 'STBK', 'STAZT', 'DSTRVM', 'DRNA', 'PHCD', 'USFL', 'MinFZ', 'MaxFZ', 'StandFNote', 'PlainRiverSort', 'COMMENTS', 'MODITIME', 'YTH', 'class']
def create_topic(topic_name, project_name):
    topic = Topic(name=topic_name)
    topic.project_name = project_name
    topic.shard_count = 1
    topic.life_cycle = 7
    topic.record_type = RecordType.TUPLE
    topic.record_schema = RecordSchema.from_lists(column_list, [FieldType.STRING, FieldType.STRING,FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.DOUBLE, FieldType.DOUBLE, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.DOUBLE, FieldType.DOUBLE, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.STRING, FieldType.DOUBLE, FieldType.DOUBLE, FieldType.DOUBLE, FieldType.STRING, FieldType.STRING, FieldType.DOUBLE, FieldType.DOUBLE, FieldType.STRING, FieldType.BIGINT, FieldType.STRING, FieldType.TIMESTAMP, FieldType.STRING, FieldType.STRING])
    try:
       dh.create_topic(topic)
       print "create topic success!"
       print "=======================================\n\n"
    except ObjectAlreadyExistException, e:
       print "topic already exist!"
       print "=======================================\n\n"
    except Exception, e:
       print traceback.format_exc()
       sys.exit(-1)

def put_data(datas):
   try:
       dh.wait_shards_ready(project_name, topic_name)
       topic = dh.get_topic(topic_name, project_name)
       shards = dh.list_shards(project_name, topic_name)
       records = []
       record = TupleRecord(schema=topic.record_schema)
       record.shard_id = shards[0].shard_id
       for data in datas:
           values = list(data)
           record_dict = dict(zip(column_list, values))
           for k, v in record_dict.items():
               if isinstance(v, Decimal):
                  v = float(v)
               elif isinstance(v, datetime):
                  v = int(time.mktime(v.timetuple()) * 1000 * 1000)
               record[k.lower()] = v
           records.append(record)
       failed_indexs = dh.put_records(project_name, topic_name, records)
       print "put tuple %d records, failed list: %s" %(len(records), failed_indexs)
   except DatahubException, e:
       print traceback.format_exc()
       sys.exit(-1)


def get_data():
   try:
       conn = pymssql.connect("db_host", "user", "passwd", "db")
       cursor = conn.cursor()
       cursor.execute('SELECT * FROM ST_STBPRP_B')
       datas = cursor.fetchall()
   except DatahubException, e:
       print traceback.format_exc()
       sys.exit(-1)
   finally:
       conn.close()
   return datas


def main():
    try:
       topic = dh.get_topic(topic_name, project_name)
       print "The topic is exsisted."
    except Exception as e:
        topic = None
        print "The topic not exsisted. creating topic"
    if not topic:
        create_topic(topic_name, project_name)
    while True:
        data = get_data()
        put_data(data)
        time.sleep(1)
if __name__ == "__main__":
    main()    
