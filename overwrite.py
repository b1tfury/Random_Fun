import sys
import os
sys.path.append('/home/serendadmin/apache-hive-0.13.1-bin/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

var1 = 'toy'
try:
  transport = TSocket.TSocket('icvs01', 9999)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  
  #client.execute("SELECT * from toy")
  query = "INSERT OVERWRITE TABLE temp_Color SELECT CASE WHEN Color='Green' THEN '1' ELSE '0' END AS ColorGreen, CASE WHEN Color='Red' THEN '1' ELSE '0' END AS ColorRed  FROM toy"
  print query
  client.execute(query)

  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
