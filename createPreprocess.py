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
  ordinal_order =     [
                     ["no-schooling","primary-schooling","junior-schooling","secondary-schooling","bachelors","masters","doctorates"],
                     ["below 55000","55000-155000","156000-365000","366000-635000","636000-865000","above 865000"],
                     ["1","2","3","above-3"],
                     ["1","2-3","3-5","above-5"],
                    ]
  lengths = [7,6,4,4]
  ordinal_features = ['Education','income','earners','residents']
  cols = ['Race','gender','marital','age','job','bank','creditcard','loan','transportation','house','net','netusage','iab']
  #cols = ['Color','Pixel']
  column_names = []
  unique_values = []
  for i in cols:
    query = "SELECT distinct(" + i +") from PROFILEINFO"
    print query
    client.execute(query)
    temp_unique = list(client.fetchAll())
    unique_values.append(temp_unique)
    k = 0
    for j in temp_unique:
        column_names.append(i + str(k))
        k = k + 1
  create_query = "CREATE TABLE PREPROCESSED ( "
  # k = 0
  for i in column_names:
    # if k == len(column_names) - 1:
    #   create_query = create_query + i + " string )"
    # else:
    create_query = create_query + i + " string,"
    # k = k + 1
  k = 0
  for i in ordinal_features:
      if k == len(ordinal_features) - 1:
          create_query = create_query + i + " string )"
      else:
          create_query = create_query + i + " string,"
          k = k + 1
  print(create_query)
  client.execute(create_query)
except Thrift.TException, tx:
  print '%s' % (tx.message)

