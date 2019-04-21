'''
Created on Jan 21, 2016

@author: Seokyong Hong
'''
import time
from abc import ABCMeta, abstractmethod
from utility.FileWriter import ResultWriter

class GraphOperation(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection
    
    @abstractmethod
    def initialize(self):
        raise RuntimeError('The initialize function must be overriden.')
    
    @abstractmethod
    def process(self):
        raise RuntimeError('The process function must be overriden.')
    
    @abstractmethod
    def store(self):
        raise RuntimeError('The store function must be overriden.')
        
    @abstractmethod
    def finalize(self):
        raise RuntimeError('The finalize function must be overriden.')
    
    def execute(self):
        start = time.time()
        
        print '[' + self.name + ']'
        
        fstart = time.time()
        self.initialize()
        print '\tInitialization Time: ' + '%f' % (time.time() - fstart)
        
        fstart = time.time()
        self.process()
        print '\tProcess Time: ' + '%f' % (time.time() - fstart)
        
        self.writer = ResultWriter(self.name)
        self.writer.open()
        fstart = time.time()        
        self.store()
        print '\tStore Time: ' + '%f' % (time.time() - fstart)
        self.writer.close()
        
        fstart = time.time()
        self.finalize()
        print '\tFinalization Time: ' + '%f' % (time.time() - fstart)
        
        print 'Execution Time: ' + '%f' % (time.time() - start)