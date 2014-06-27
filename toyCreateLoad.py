import sys
import os
sys.path.append('/home/serendadmin/apache-hive-0.13.1-bin/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
  transport = TSocket.TSocket('icvs01', 9999)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  client.execute("CREATE TABLE IF NOT EXISTS TOY (Color string,Pixel string,Education string,income string) comment 'Delete this later' row format delimited FIELDS TERMINATED BY ',' stored as textfile ")
  client.execute("LOAD DATA INPATH '/home/serendadmin/azhar/color.csv' OVERWRITE INTO TABLE TOY")
  while (1):
    row = client.fetchOne()
    if (row == None):
       break
    print row

  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
