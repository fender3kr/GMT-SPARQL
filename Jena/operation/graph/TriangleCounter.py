'''
Created on Jan 21, 2016

@author: Seokyong Hong
'''
from threading import Thread
from operation.GraphOperation import GraphOperation

counter = []

class Worker(Thread):
    def __init__(self, name, query, connection):
        Thread.__init__(self)
        self.name = name
        self.query = query
        self.connection = connection
    
    def run(self):
        global counter
        
        results = self.connection.urika.query(self.name, self.query, None, None, 'json', True)
        counter.append(int(results['results']['bindings'][0]['count']['value']))

class TriangleCounter(GraphOperation):
    COMMAND_TO_INITIALIZE = '''
            CREATE GRAPH <http://workingGraph>;
                            '''
    COMMAND_TO_FINALIZE = '''
            DROP GRAPH <http://workingGraph>;
                          '''
    COMMAND_TO_STORE_INITIAL_GRAPH = '''
            INSERT { GRAPH <http://workingGraph> { ?node <temp:links> ?neighbor }}
            WHERE {
                SELECT ?node ?neighbor
                WHERE {
                    ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor .
                    FILTER (STR(?node) < STR(?neighbor))
                }
            };
                                     '''
    COMMAND_TO_COUNT_TRIANGLES = '''
            SELECT (COUNT(DISTINCT *) AS ?count)
            WHERE {
                GRAPH <http://workingGraph> {
                     ?x <temp:links> ?y .
                     ?y <temp:links> ?z .
                     ?x <temp:links> ?z
                }
            }
                                 '''
    COMMAND_TO_COUNT_TRIANGLES_DIRECTED_SINGLE = '''
            SELECT (COUNT(*) AS ?count)
                WHERE {
                    {
                        ?x ?a ?y .
                        ?y ?b ?z .
                        ?z ?c ?x .
                        FILTER (STR(?x) < STR(?y)) .
                        FILTER (STR(?y) < STR(?z))
                    }
                    UNION 
                    {
                        ?x ?a ?y .
                        ?y ?b ?z .
                        ?z ?c ?x .
                        FILTER (STR(?y) > STR(?z)) .
                        FILTER (STR(?z) > STR(?x))
                    }  
                    UNION 
                    {
                        ?x ?a ?y .
                        ?y ?b ?z .
                        ?x ?c ?z
                    }
                }
                                                 '''
    COMMANDS_FOR_MULTITHREADS = [
                  '''
            SELECT (COUNT(*) AS ?count)
            WHERE {
                ?x ?a ?y .
                ?y ?b ?z .
                ?z ?c ?x .
                FILTER (STR(?x) < STR(?y)) .
                FILTER (STR(?y) < STR(?z))
            }
                  '''
                  ,
                  '''
            SELECT (COUNT(*) AS ?count)
            WHERE {
                ?x ?a ?y .
                ?y ?b ?z .
                ?z ?c ?x .
                FILTER (STR(?y) > STR(?z)) .
                FILTER (STR(?z) > STR(?x)) 
            }
                  '''
                  ,
                  '''
            SELECT (COUNT(*) AS ?count)
            WHERE {
                ?x ?a ?y .
                ?y ?b ?z .
                ?x ?c ?z
            }
                  '''
    ] 
    
    def __init__(self, directed, multithreaded, connection):
        GraphOperation.__init__(self, 'Triangle Counter', connection)
        self.directed = directed
        self.multithreaded = multithreaded
    
    def initialize(self):
        if self.directed == False:
            self.connection.urika.update(self.name, TriangleCounter.COMMAND_TO_INITIALIZE)
        else:
            if self.multithreaded == True:
                self.workers = []
                for worker in range(0, 3):
                    self.workers.append(Worker('Worker-' + str(worker + 1), TriangleCounter.COMMANDS_FOR_MULTITHREADS[worker], self.connection))
            
    def process(self):
        if self.directed == False:
            self.connection.urika.update(self.name, TriangleCounter.COMMAND_TO_STORE_INITIAL_GRAPH)
            results = self.connection.urika.query(self.name, TriangleCounter.COMMAND_TO_COUNT_TRIANGLES, None, None, 'json', True)
            self.count = results['results']['bindings'][0]['count']['value']
        else:
            if self.multithreaded == False:
                results = self.connection.urika.query(self.name, TriangleCounter.COMMAND_TO_COUNT_TRIANGLES_DIRECTED_SINGLE, None, None, 'json', True)
                self.count = results['results']['bindings'][0]['count']['value']
            else:
                for worker in self.workers:
                    worker.start();
            
                for worker in self.workers:
                    worker.join()
                
                global counter
                self.count = 0
                for count in counter:
                    self.count = self.count + count
       
    def store(self):
        if self.writer is not None:
            self.writer.writeLine('Number of triangles: ' + str(self.count))
    
    def finalize(self):
        if self.directed == False:
            self.connection.urika.update(self.name, TriangleCounter.COMMAND_TO_FINALIZE)