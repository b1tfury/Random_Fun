import sys
import os
sys.path.append('/home/serendadmin/apache-hive-0.13.1-bin/lib/py')

from hive_service import ThriftHive

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

var2 = 'temp_Color'
var1 = 'temp_Pixel'
try:
  transport = TSocket.TSocket('icvs01', 9999)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ThriftHive.Client(protocol)
  transport.open()
  
  #client.execute("SELECT * from toy")
  #query = "INSERT OVERWRITE TABLE bigtoy select temp_Color.*,temp_Pixel.* form temp_Color,temp_Pixel"
  #query = "INSERT OVERWRITE TABLE bigtoy SELECT t.ColorGreen as ColorGreen, t.ColorRed as ColorRed, p.Pixel123 as Pixel123, p.Pixel777 as Pixel777, p.Pixel444 as Pixel444 from temp_Color t , temp_Pixel p"
  query = "INSERT OVERWRITE TABLE bigtoy SELECT temp_Color.ColorGreen as ColorGreen, temp_Color.ColorRed as ColorRed, temp_Pixel.Pixel123 as Pixel123, temp_Pixel.Pixel777, temp_Pixel.Pixel444 as Pixel444 from temp_Color , temp_Pixel"
  print query
  client.execute(query)

  transport.close()
except Thrift.TException, tx:
  print '%s' % (tx.message)
