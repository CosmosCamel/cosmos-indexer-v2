#!/usr/bin/env python
# -*- coding: utf-8 -*- 
#CamelJuno 🐪

"""
We fetch blocks by a range of block heights, write the block timestamp and transaction bytes to a file in directory named blocks, at the highest possible speeds per cpu core.
Can divide the input over multiple instances of this script to use up to 100% of CPU
"""

"""
Runs at 8700 Blocks/Min on a Ryzen 9 3900X - Last tested on 14th April 2023
"""

import requests
import json
import os
import time
import random
from colorama import init,Fore,Back,Style
init(autoreset=True)
from multiprocessing.dummy import Pool as ThreadPool
import threading
from threading import *
from queue import Queue

# create custom worker as daemon
class Worker(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()

# ThreadPool instantiates workers and maps arguments with tasks to each
class ThreadPool:
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        self.tasks.join()

#Total Tx Count
totalTx = 0
#Starting timestamp to calculate processed blocks per minute
startTime = 0
#FinishedBlocks
blocksNo = 0

def fetchBlockData(height):
    headers = {
        'Host': 'rpc-archive.junonetwork.io',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    if height > 4136532:
        a = requests.get("https://rpc-archive.junonetwork.io/block?height="+str(height),headers=headers, timeout=10)
        if a.status_code == 200:
            return json.loads(a.text)['result']
        else:
            fetchBlockData(height)
    elif height > 2578097:
        headers['Host'] = 'rpc-v3-archive.junonetwork.io'
        a = requests.get("https://rpc-v3-archive.junonetwork.io/block?height="+str(height),headers=headers,timeout=10)
        if a.status_code == 200:
            return json.loads(a.text)['result']
        else:
            fetchBlockData(height)
    else:
        headers['Host'] = 'rpc-v2-archive.junonetwork.io'
        a = requests.get("https://rpc-v2-archive.junonetwork.io/block?height="+str(height),headers=headers,timeout=10)
        if a.status_code == 200:
            return json.loads(a.text)['result']
        else:
            fetchBlockData(height)

def scanStats(height):
    try:
        global totalTx
        global blocksNo
        global datablock
        #change terminal title instead of CLI printing
        os.system('title Blocks: '+str(blocksNo)+' - Txs: '+str(totalTx)+' - BPM: '+str(int(round(blocksNo/((time.time()-startTime)/60)))))
        #check if height was already done, avoiding conflict of more than one thread executing the same input
        if (os.path.isfile('blocks/'+str(height)+'.json')):
            return
        #fetch block Data
        blockData = fetchBlockData(height)
        #get transactions
        txs = blockData["block"]["data"]["txs"]
        #if block has transactions
        if len(txs) > 0:
            #open file
            with open("blocks/"+str(height), "w+") as j:
                #write block time to file
                j.write(blockData["block"]["header"]["time"]+'\n')
                for i in range(len(txs)):
                    #ignore amino longer than 30k
                    if len(txs[i]) <= 32766:
                        #write amino to file
                        j.write(txs[i]+'\n')
        totalTx = totalTx + len(txs)
        blocksNo = blocksNo + 1
        os.system('title Blocks: '+str(blocksNo)+' - Txs: '+str(totalTx)+' - BPM: '+str(int(round(blocksNo/((time.time()-startTime)/60)))))
    except Exception as e:
        print(e)
        pass

def startPool(data):
    try:
        #these are not logical processes so you dont have to match them with your physical threads, but a decent cap is 100 per every running instance
        pool = ThreadPool(100)
        pool.map(scanStats,data)
        pool.wait_completion()
        pool.close() 
        pool.join()
    except(KeyboardInterrupt,EOFError):
        pool.wait_completion()
    except(AttributeError):
        pass

if __name__ == "__main__":
    #list of block height range
    data = list(range(1,7822133))
    #remove non existent heights
    data.remove(2578098)
    data.remove(4136531)
    #shuffle in random order to distribute load over the 3 archive nodes
    random.shuffle(data)
    #create folder named blocks if it doesnt exist
    if(not(os.path.isdir('blocks'))):
        os.makedirs('blocks')
    print(Fore.WHITE+Style.BRIGHT+"\nNumber of blocks to query: "+Fore.GREEN+Style.BRIGHT+str(len(data))+"\n")
    #timestamp of runtime start
    startTime = time.time()
    #launch the threadpool
    startPool(data)
    print(Fore.WHITE+Style.BRIGHT+"Total number of processed Transactions: "+Fore.GREEN+Style.BRIGHT+str(totalTx)+"\n")