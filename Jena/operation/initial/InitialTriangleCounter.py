'''
Created on Jan 21, 2016

@author: Seokyong Hong
'''
from operation.GraphOperation import GraphOperation

class InitialTriangleCounter(GraphOperation):
    COMMAND_TO_COUNT_TRIANGLES = '''
            SELECT (COUNT(DISTINCT *) AS ?count) 
            WHERE {
                {?x ?p ?y} UNION {?y ?p ?x} .
                {?y ?p ?z} UNION {?z ?p ?y} .
                {?z ?p ?x} UNION {?x ?p ?z} .
                FILTER(STR(?x) < STR(?y)) .
                FILTER(STR(?y) < STR(?z)) 
            }
                                 '''

    def __init__(self, connection):
        GraphOperation.__init__(self, 'Initial Triangle Counter', connection)
    
    def initialize(self):
        pass
            
    def process(self):
        results = self.connection.urika.query(self.name, InitialTriangleCounter.COMMAND_TO_COUNT_TRIANGLES, None, None, 'json', True)
        self.count = results['results']['bindings'][0]['count']['value']
       
    def store(self):
        if self.writer is not None:
            self.writer.writeLine('Number of triangles: ' + self.count)
    
    def finalize(self):
        pass