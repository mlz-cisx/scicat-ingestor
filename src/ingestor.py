#!/usr/bin/env python3
import os
import dataset
import datablock
import requests

from  datetime import datetime
from dateutil.relativedelta import relativedelta
DEF_URL = "https://scicat-dev.k8s-test.frm2.tum.de/"

class Ingestor:

    def __init__(self, url=DEF_URL):
        self.url = url

    def login(self, user="ingestor", passwd="aman"):
        r=requests.post( self.url +"api/v3/Users/login", json = {"username":user, "password":passwd})
        print (r.text)
        self.token = r.json()['access_token']

    def createDataset(self):
        now =datetime.now()
        np3y = now + relativedelta(years=3)

        d = dataset.Dataset()
          
        d.fields.datasetName ="Testdataset"
        d.fields.creationLocation ='MLZ'
        d.fields.owner="63e24d771353236f66a61d31"
        d.fields.principalInvestigator="Björn Pedersen"
        d.fields.contactEmail="bjoern.pedersen@frm2.tum.de"
        d.fields.ownerEmail="bjoern.pedersen@frm2.tum.de"
        d.fields.sourceFolder = "/some/folder"
        d.fields.creationTime=now.isoformat()
        d.fields.endTime=now.isoformat()
        d.fields.type="raw"
        d.fields.ownerGroup="orgasoft"
        d.fields.accessGroups=["mlz","resi"]
        d.fields.datasetlifecycle.dateOfDiskPurging = np3y.isoformat()
        d.fields.datasetlifecycle.archiveRetentionTime = np3y.isoformat()
        d.fields.datasetlifecycle.dateOfPublishing = np3y.isoformat()
        d.fields.isPublished=False
        d.fields.scientificMetadata= {"info": {"value": "test","unit": ""}}

        j = d._toJson()

        print(j)
        r = requests.post( f'{self.url}api/v3/Datasets', headers= {"Authorization": f"Bearer {self.token}"}, json=j )
        print(r.text)
        self.pid = r.json()['pid']

    def addDataBlock(self, flist):
       b = datablock.origdatablock()
       for f in flist:
           fi = datablock.datafile(f, datetime.now().isoformat())
           fi.size = 200

           b.dataFileList.append(fi)

       b.size =len(b.dataFileList)
       j = datablock.asdict(b)
       print(j)

       upid = requests.utils.quote(self.pid, safe='')
       r = requests.post( f'{self.url}api/v3/Datasets/{upid}/origdatablocks/', json=j, headers= {"Authorization": f"Bearer {self.token}"})
       print(r.text) 


def run():
    i = Ingestor(os.environ.get('SCICAT_URL'))
    if os.environ.get('SCICAT_TOKEN'):
        i.token = os.environ.get('SCICAT_TOKEN')
    else:
        i.login(os.environ.get('SCICAT_USER'),os.environ.get('SCICAT_PASSWORD'))
    i.createDataset()
    i.addDataBlock(['file1.xxx','file2.yyy'])

if __name__=='__main__':
    run()
