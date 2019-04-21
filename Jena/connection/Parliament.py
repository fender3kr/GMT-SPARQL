'''
Created on Jan 24, 2016

@author: Seokyong Hong
'''
from SPARQLWrapper.Wrapper import SPARQLWrapper, JSON, POST

class Parliament(object):
    class Urika(object):
        def __init__(self, wrapper):
            self.wrapper = wrapper
        
        def query(self, name, query, dum1, dum2, format, dum3):
             self.wrapper.setQuery(query)
             self.wrapper.setReturnFormat(JSON)
             
             return self.wrapper.query().convert()
        
        def update(self, name, query):
            self.wrapper.setQuery(query)
            self.wrapper.setMethod(POST)
            ret = self.wrapper.query()

    def __init__(self, url, port):
        interface = url + ':' + str(port) + '/parliament/sparql' 
        wrapper = SPARQLWrapper(interface)
        self.urika = Parliament.Urika(wrapper)
        
        