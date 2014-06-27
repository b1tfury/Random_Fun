import sys
import os
sys.path.append('/usr/local/hive/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
  transport = TSocket.TSocket('localhost', 10000)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  
  client.execute("INSERT OVERWRITE DIRECTORY 'hdfs:///user/sahil/data/' SELECT * FROM preprocessed")

except Thrift.TException, tx:
  print '%s' % (tx.message)
