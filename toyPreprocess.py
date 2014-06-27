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
  #ordinal_features = ['Education','income','earners','residents']
  ordinal_features = ['Education','income']
  #cols = ['Race','gender','marital','age','job','bank','creditcard','loan','transportation','house','net','netusage','iab']
  cols = ['Color','Pixel']
  column_names = []
  unique_values = []
  for i in cols:
    query = "SELECT distinct(" + i +") from TOY"
    print query
    client.execute(query)
    temp_unique = list(client.fetchAll())
    unique_values.append(temp_unique)
    k = 0
    for j in temp_unique:
        column_names.append(i + str(k))
        k = k + 1
  create_query = "CREATE TABLE TOY_PREPROCESSED ( "
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


  insert_query = "INSERT OVERWRITE TABLE TOY_PREPROCESSED SELECT "
  k = 0
  totalColumns = len(column_names)
  count = 0
  for i in cols:
      for j in unique_values[k]:
          # if count == totalColumns - 1:
          #     insert_query = insert_query + "CASE WHEN " + i + "= '" + j + "' THEN '1' ELSE '0' END AS " + column_names[count] + " FROM PROFILEINFO"
          # else:
        insert_query = insert_query + "CASE WHEN " + i + "= '" + j + "' THEN '1' ELSE '0' END AS " + column_names[count] + ", "
        count = count + 1
      k = k + 1
  count = 1
  j = 0
  for i in range(0,len(ordinal_features)):
      insert_query = insert_query + "CASE " + ordinal_features[i]
      k = 1
      for feature in ordinal_order[i]:
          if i == (len(ordinal_features) - 1) and k == (len(ordinal_order[i])):
              print i,len(ordinal_features) - 1,k,len(ordinal_order[i])
              insert_query = insert_query + " ELSE " + str((ordinal_order[i].index(feature) + 0.5 )/len(ordinal_order[i])) +" END AS " + ordinal_features[i] + " FROM TOY "
          elif k == (len(ordinal_order[i])):
              insert_query = insert_query + " ELSE " + str((ordinal_order[i].index(feature) + 0.5 )/len(ordinal_order[i])) + " END AS " + ordinal_features[i] +","
          else :
              insert_query = insert_query + " WHEN '" + feature +"' THEN " + str((ordinal_order[i].index(feature) + 0.5)/len(ordinal_order[i])) +" "
              k = k + 1
          #print ordinal_order[i]
          #print str(ordinal_order[i].index(feature)/len(ordinal_order[i]))
          #print feature
          #print ordinal_order[i].index(feature)
          #print len(ordinal_order[i])
          #print ordinal_order[i].index(feature)/len(ordinal_order[i])
  print (insert_query)
  client.execute(insert_query)
  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)

