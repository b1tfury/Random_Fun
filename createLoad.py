import sys
import os
sys.path.append('/usr/local/hive/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
  print "sahhil"
  transport = TSocket.TSocket('localhost', 10000)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  client.execute("CREATE EXTERNAL TABLE IF NOT EXISTS PROFILEINFO (custid string,Race string,Education string,gender string,marital string,age string,job string,income string,earners string,residents string,bank string,creditcard string,loan string,transportation string,house string,net string,netusage string,catmac string,iab string) comment 'Delete this later' row format delimited FIELDS TERMINATED BY ','  stored as textfile location 'hdfs:///user/sahil/output/cassandra'") 
 # client.execute("LOAD DATA INPATH 'hdfs:///user/sahil/input/profileinfo99.csv' OVERWRITE INTO TABLE PROFILEINFO")
  #while (1):
  #  row = client.fetchOne()
   # if (row == None):
   #    break
   # print row

  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
