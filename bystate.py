#!/usr/bin/env python

import multiprocessing as mp
import os.path as osp
from glob import glob
import json

thisfile = osp.abspath(__file__)
thisdir  = osp.dirname(thisfile)
statedir = osp.join(thisdir,'states')
_statewc = osp.join(statedir,'??')
states_q = [(osp.basename(i),i) for i in glob(_statewc)]

nprocs = mp.cpu_count()-1

def featureagg(st,path):
    jsonfiles = glob(osp.join(path,'?????.json'))
    D = dict.fromkeys(['type', 'features'])
    D['type']='FeatureCollection'
    D['features'] = []
    for fn in jsonfiles:
      f=open(fn,'r')
      D['features'].extend(json.load(f)['features'])
      f.close()
    p = osp.join(statedir,st,"%s.json"%st)
    with open(p,'w') as f:
      json.dump(D,f)
    return p


def worker(input, output):
  for st,path in iter(input.get, 'STOP'):
    result = featureagg(st,path)
    output.put(result)

def createstatefiles():
  todo = mp.Queue()
  done = mp.Queue()

  for st in states_q:
    todo.put(st)

  for i in xrange(nprocs):
    mp.Process(target=worker, args=(todo, done)).start()

  for i in states_q:
    print done.get()
  
  for i in range(nprocs):
    todo.put('STOP')

if __name__=='__main__':
  mp.freeze_support()
  createstatefiles()
  
