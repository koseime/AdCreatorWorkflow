import sys
import csv
import optparse
import string
from string import *

import time
from time import *

parser = optparse.OptionParser()

parser.add_option('-f', '--file', help='input file name')
parser.add_option('-d', '--input_delimiter', default='\t',help='input delimiter (Tab by defautl)')
parser.add_option('-o', '--output_delimiter', default='\t',help='output delimiter (Tab by defautl)')
parser.add_option('-c', '--column', help='list of column',dest="column", action="store_true")
parser.add_option('-e', '--epoch', help='add epoch column at the begining of every line from the two first columns',dest="time_col", action="store_true")


(opts, args) = parser.parse_args()

if opts.file:
    inf = csv.reader(open(opts.file,"rb"),delimiter=opts.input_delimiter)
else:
    inf = sys.stdin

## FOR TESTING PURPOSE (IGNORE ARGUMENTS)
# opts.input_delimiter=','
# inf = csv.reader(open('samplea.csv',"rb"),delimiter=opts.input_delimiter)
# args = ['date','time','dhost', 'app', 'cnt']

fields_available = {}
for arg_key in args:
    fields_available[arg_key] = 1

fields_arr ="headerTimestamp,headerHost,headerCefVersion,headerDeviceVendor,headerDeviceProduct,headerDeviceVersion,headerSignatureId,headerName,headerSeverity,deviceAction,applicationProtocol,deviceEventCategory,baseEventCount,destinationHostName,destinationProcessId,destinationMacAddress,destinationNtDomain,destinationUserPrivileges,destinationProcessName,destinationPort,destinationAddress,destinationUserId,destinationUserName,deviceAddress,deviceHostName,deviceProcessId,endTime,fileName,fileSize,bytesIn,message,eventOutcome,bytesOut,transportProtocol,requestURL,receiptTime,sourceHostName,sourceMacAddress,sourceNtDomain,sourceProcessId,sourceProcessName,sourcePort,sourceUserPrivileges,sourceAddress,startTime,sourceUserId,sourceUserName,deviceCustomString1,deviceCustomString2,deviceCustomString3,deviceCustomString4,destinationDnsDomain,destinationServiceName,destinationTranslatedAddress,destinationTranslatedPort,deviceDirection,deviceDnsDomain,deviceExternalId,deviceFacility,deviceInboundInterface,deviceMacAddress,deviceNtDomain,deviceOutboundInterface,deviceProcessName,deviceTranslatedAddress,externalId,fileCreateTime,fileHash,fileId,fileModificationTime,filePath,filePermission,fileType,oldFileCreateTime,oldFileHash,oldFileId,oldFileModificationTime,oldFileName,oldFilePath,oldFilePermission,oldFileSize,oldFileType,reason,requestClientApplication,requestCookies,requestMethod,sourceDnsDomain,sourceServiceName,sourceTranslatedAddress,sourceTranslatedPort,headerEpochTimestamp,null".split(",") 

myDictionary ={}
id = 0
for field in fields_arr:
    myDictionary[field] = id
    id += 1


id2id = {}
ii = 0
myList = []
jj = 0
for field in fields_arr:
    if (fields_available.has_key(field) == True):
		myDictionary[field] = ':' + str(myDictionary[field]) + ':'+opts.output_delimiter
   		jj += 1
    else:
        myDictionary[field] = opts.output_delimiter


ii = 0
for key in args:
	id2id[ii] = atoi(myDictionary[key]. replace(':','').replace(opts.output_delimiter,''))
	ii += 1
        
myString = ''
for field in fields_arr:
	if field != "null":
		myString += myDictionary[field]

time_col = [0,1]                
for row in inf:
 	
	if (inf == sys.stdin):
		row = row.strip()
		row = row.split(opts.input_delimiter)
	ii = 0
	if opts.time_col:
		try:
			epoch = str(int(mktime(strptime(row[time_col[0]]+row[time_col[1]],'%m/%d/%Y%H:%M:%S'))))
		except:
			epoch=''
	myCurrString = myString
	for item in row:
		try:
			key = ':' + str(id2id[ii]) + ':'
			myCurrString = myCurrString.replace(key, item)
		except:
			jj = 0
		ii += 1
        
       	if opts.time_col: 
			myCurrString = epoch+opts.output_delimiter+myCurrString 
	print (myCurrString[:-1])



