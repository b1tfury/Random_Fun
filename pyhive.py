#!/usr/bin/env python
import sys
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
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
 
except Thrift.TException, tx:
    print '%s' % (tx.message)
