import sys
import os
sys.path.append('/usr/local/hive/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

var2 = 'temp_Color'
var1 = 'temp_Pixel'
var1 = 'PREPROCESSED'
var4 = 'temp_iab'
try:
  transport = TSocket.TSocket('localhost', 10000)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  
  #client.execute("SELECT * from toy")
  query = "SELECT * FROM " + var1
  print query
  client.execute(query)

  while (1):
    row = client.fetchOne()
    if (row == None):
       break
    print row

  query = "SELECT * FROM " + var2
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
