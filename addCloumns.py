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
  cols = ['Color','Pixel']
  #client.execute("SELECT distinct(Color) from toy")
  #colors = list(client.fetchAll())
  #extra_cols = []
  for i in cols:
    query = "SELECT distinct(" + i +") from toy"
    print query
    client.execute(query)
    extra_cols = list(client.fetchAll())
    alter_query = "ALTER TABLE toy ADD COLUMNS( "
    k = 0
    for j in extra_cols:
      if k == len(extra_cols) - 1:
        alter_query = alter_query + i+j + " string )"
      else:
        alter_query = alter_query + i+j + " string,"
      k = k + 1
    print alter_query
    client.execute(alter_query)
  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
