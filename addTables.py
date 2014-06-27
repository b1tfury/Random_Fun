
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
  cols = ['Race','gender','marital','age','job','bank','creditcard','loan','transportation','house','net','netusage','iab']
  #cols = ['iab']
  #client.execute("SELECT distinct(Color) from toy")
  #colors = list(client.fetchAll())
  #extra_cols = []
  deleteTables = []
  columnList = []
  columnNames = []
  for i in cols:
    query = "SELECT distinct(" + i +") from PROFILEINFO"
    #print query
    client.execute(query)
    temp_cols = []
    extra_cols = list(client.fetchAll())
    table_name = 'temp_'+i
    deleteTables.append(table_name)
    create_query = "CREATE TABLE " + table_name +"("
    insert_query = "INSERT OVERWRITE TABLE " + table_name + " SELECT "
    k = 0
    for j in extra_cols:
      if k == len(extra_cols) - 1:
        #create_query = create_query + i+j + " string )"
        create_query = create_query + i+str(k) + " string )"
        #columnList.append(i+j)
        #temp_cols.append(i+j)
        columnList.append(i+str(k))
        temp_cols.append(i+str(k))
        #insert_query = insert_query + "CASE WHEN " + i + "='" + j +"' THEN '1' ELSE '0' END AS " + i+j + " FROM PROFILEINFO"
        insert_query = insert_query + "CASE WHEN " + i + "='" + j +"' THEN '1' ELSE '0' END AS " + i+str(k) + " FROM PROFILEINFO"
      else:
        #create_query = create_query + i+j + " string,"
        create_query = create_query + i+str(k) + " string,"
        #columnList.append(i+j)
        #temp_cols.append(i+j)
        columnList.append(i+str(k))
        temp_cols.append(i+str(k))
       #insert_query = insert_query + "CASE WHEN " + i + "='" + j +"' THEN '1' ELSE '0'	END AS " + i+j + ","
        insert_query = insert_query + "CASE WHEN " + i + "='" + j +"' THEN '1' ELSE '0' END AS " + i+str(k) + ","
      k = k + 1
    columnNames.append(temp_cols)
    print create_query
    client.execute(create_query)
    print insert_query
    client.execute(insert_query)
  final_query = "CREATE TABLE PREPROCESSED ("
  k = 0
  for i in columnList:
    if k == len(columnList) - 1:
      final_query = final_query + i + " string )"
    else:
      final_query = final_query + i + " string,"
    k = k + 1
  client.execute(final_query)
  print final_query
  print deleteTables
  print columnNames
  print columnList
  final_query_sure = "INSERT OVERWRITE TABLE PREPROCESSED SELECT "
  k = 0
  totalColumns = len(columnList)
  count = 1
  tables = ' '
  for i in deleteTables:
    if k == len(deleteTables)-1:
      tables = tables + i
    else:
      tables = tables + i +","
    for j in columnNames[k]:
      if count == totalColumns:
        final_query_sure = final_query_sure + i+"."+j+" as " + j + " from " + tables
        count = count + 1
      else:
        final_query_sure = final_query_sure + i+"."+j+" as " + j + ","
        count = count + 1
    k = k + 1
  print final_query_sure
  client.execute(final_query_sure)

  #for i in deleteTables:
   # client.execute("drop table " + i)
  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
