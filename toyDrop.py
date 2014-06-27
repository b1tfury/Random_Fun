
import sys
import os
sys.path.append('/home/serendadmin/apache-hive-0.13.1-bin/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

var1 = 'TOY'
var2 = 'TOY_PREPROCESSED'
var3 = 'toy'
var4 = 'bigtoy'
try:
  transport = TSocket.TSocket('icvs01', 9999)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  
  #client.execute("SELECT * from toy")
  query = "DROP TABLE " + var1
  print query
  client.execute(query)

  query = "DROP TABLE " + var2
  print query
  client.execute(query)

  query = "DROP TABLE " + var3
  print query
  client.execute(query)

  query = "DROP TABLE " + var4
  print query
  client.execute(query)


  while (1):
    row = client.fetchOne()
    if (row == None):
       break
    print row

  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
