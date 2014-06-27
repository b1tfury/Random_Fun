import sys
import os
import timeit

sys.path.append('/usr/local/hive/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

start = timeit.default_timer()
try:
  transport = TSocket.TSocket('localhost', 10000)
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
  ordinal_features = ['education','income','earners','residents']
  #ordinal_features = ['education','income']
  cols = ['Race','gender','marital','age','job','bank','creditcard','loan','transportation','house','net','netusage','iab']
  #cols = ['Color','Pixel']
  column_names = []
  unique_values = []
  for i in cols:
    query = "SELECT distinct(" + i +") from PROFILEINFO"
    print query
    client.execute(query)
    temp_unique = list(client.fetchAll())
    print (i, ":" ,temp_unique)
    unique_values.append(temp_unique)
    k = 0
    for j in temp_unique:
        column_names.append(i + str(k))
        k = k + 1
  create_query = "CREATE TABLE PREPROCESSED ( "
  for i in column_names:
    create_query = create_query + i + " string,"
  k = 0
  for i in ordinal_features:
      if k == len(ordinal_features) - 1:
          create_query = create_query + i + " string ) row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile location 'hdfs:///user/sahil/final'"
      else:
          create_query = create_query + i + " string,"
          k = k + 1
  print(create_query)
  client.execute(create_query)


  insert_query = "INSERT INTO TABLE PREPROCESSED SELECT "
  k = 0
  totalColumns = len(column_names)
  count = 0
  for i in cols:
      for j in unique_values[k]:
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
              insert_query = insert_query + " ELSE " + str((ordinal_order[i].index(feature) + 0.5 )/len(ordinal_order[i])) +" END AS " + ordinal_features[i] + " FROM PROFILEINFO "
          elif k == (len(ordinal_order[i])):
              insert_query = insert_query + " ELSE " + str((ordinal_order[i].index(feature) + 0.5 )/len(ordinal_order[i])) + " END AS " + ordinal_features[i] +","
          else :
              insert_query = insert_query + " WHEN '" + feature +"' THEN " + str((ordinal_order[i].index(feature) + 0.5)/len(ordinal_order[i])) +" "
              k = k + 1
  print (insert_query)
  client.execute(insert_query)
  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)

stop = timeit.default_timer()
time_taken = stop - start
print "Time taken:",time_taken

