import os
import sys
import time
from threading import Thread
from SimpleDaemon import SimpleDaemon
from readMDA import readMDA
from epics import PV
import redis

class MDAConvert():
    """
    An object that watches for a new MDA file and converts it to columnar ascii and saves to users directory
    """

    def __init__(self):

        self.save_record = 'SR13ID01HU02IOC02:saveData_'
        self.scan_record = 'SR13ID01HU02IOC02:scan1.'
        self.mda_path = '/data/ioc2/'
        self.dataMount = '/data/pilatus1M/'
        self.dataSubDirectory = 'scan'
        self.dataFullPath = ''
        self.experimentPathPV = PV('13PIL1:cam1:FilePath_RBV',callback=self.onExpFilePath)
        self.scanDataStatePV = PV('%sDSTATE' % (self.scan_record,), callback=self.onScanDSTATE)
        self.scanFileNamePV = PV('%sfileName' % (self.save_record,))
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)
         
    def onExpFilePath(self, pvname, value, char_value, **kwargs):
        self.dataFullPath = '%s%s/%s/' % (self.dataMount,'/'.join(char_value.split('/')[-5:-2]),self.dataSubDirectory)
    
    def onScanDSTATE(self, pvname, value, **kwargs):
        print 'DSTATE'
        if value  == 7:
            thread=Thread(target=self.convertMDA)
            thread.daemon = True
            thread.start()
        
    def convertMDA(self):
        mdaFile = '%s%s' % (self.mda_path,self.scanFileNamePV.get(as_string=True))
        
        print mdaFile
        
        try:
            mda = readMDA(mdaFile, verbose=False)
        except Exception:
            return
        
        data = mda[-1]
        results = [p.data for p in data.p]
        results.extend([d.data for d in data.d])
        results = zip(*results)
        names = [p.name for p in data.p]
        names.extend([d.name for d in data.d])
    
        datFileName = '%s%s.csv' % (self.dataFullPath, os.path.splitext(os.path.basename(mdaFile))[0])
        
        print datFileName
        
        with open(datFileName,'w') as f:
            print >> f, ', '.join('%s' % name for name in names)
            for line in results:
                print >> f, ', '.join('%s' % data for data in line)     

        epn = self.dataFullPath.split('/')[-3]
        try:
            self.redis.rpush('MDA:%s:%s' % (epn,os.path.splitext(os.path.basename(datFileName))[0]),self.datFileName)
        except redis.ConnectionError:
            print 'Error connecting to redis database'
        

class MDAConvertDaemon(SimpleDaemon):
    # daemon method
    def run(self):
        from MDAConvert import MDAConvert
        a = MDAConvert()
        while True:
            time.sleep(0.1)
        
        print 'Done'


if __name__ == "__main__":
    MDAConvertDaemon()