'''
Created on Jan 21, 2016

@author: phantom
'''
from operation.GraphOperation import GraphOperation

class NodeEccentricity(GraphOperation):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = '''
            CREATE GRAPH <http://workingGraph>;
                                          '''
    COMMAND_TO_FINALIZE_WORKING_GRAPH = '''
            DROP GRAPH <http://workingGraph>;
                                        '''
    COMMAND_TO_INSERT_INITIAL_NODE = '''
            INSERT {{ GRAPH <http://workingGraph> {{ {NODE} <temp:labels> {INITIAL_VALUE} . }}}} 
            WHERE {{
            }};
                                     '''
    COMMAND_TO_INSERT_NEIGHBOR_NODES = '''
            INSERT {{ GRAPH <http://workingGraph> {{ ?neighbor <temp:labels> {0} }} }}
            WHERE {{
                SELECT ?neighbor ?label
                WHERE 
                {{
                    GRAPH <http://workingGraph> {{ 
                            ?node <temp:labels> {1} 
                    }} .
                    ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor .
                    NOT EXISTS {{ GRAPH <http://workingGraph> {{ ?neighbor <temp:labels> ?any . }} }}
                }}
            }}           
                                        '''
    COMMAND_TO_COUNT_NODES_IN_TEMPORARY_GRAPH = '''
            SELECT (COUNT(*) AS ?count) 
            WHERE { 
                GRAPH <http://workingGraph> { 
                    ?s ?p ?o 
                }
            }
                                              '''
    COMMAND_TO_COUNT_NODES = '''
            SELECT (COUNT(DISTINCT ?node) AS ?count)
            WHERE {
                ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor
            }        
                             '''

    def __init__(self, node, connection):
        GraphOperation.__init__(self, 'Node Eccentricity', connection)
        self.node = node
    
    def initialize(self):
        self.connection.urika.update(self.name, NodeEccentricity.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
        self.connection.urika.update(self.name, NodeEccentricity.COMMAND_TO_INSERT_INITIAL_NODE.format(NODE = self.node, INITIAL_VALUE = 0))
            
    def process(self):
        self.iteration = 0
        self.count = 0
        shouldStop = False
        previousCount = self.counts()
        
        while not shouldStop:
            self.connection.urika.update(self.name, NodeEccentricity.COMMAND_TO_INSERT_NEIGHBOR_NODES.format(self.iteration + 1, self.iteration))
            currentCount = self.counts()
        
            if previousCount == currentCount:
                self.count = currentCount
                shouldStop = True
                break
            
            previousCount = currentCount
            self.iteration = self.iteration + 1
       
    def store(self):
        print '\tNumber of Iteration: ' + str(self.iteration + 1)
        
        if self.writer is not None:
            if self.count == self.countNodes():
                self.writer.writeLine('The eccentricity of ' + self.node + ' is ' + str(self.iteration) + '.')
            else:
                self.writer.writeLine('The eccentricity of ' + self.node + ' is infinite.')
    
    def finalize(self):
        self.connection.urika.update(self.name, NodeEccentricity.COMMAND_TO_FINALIZE_WORKING_GRAPH)
    
    def counts(self):
        results = self.connection.urika.query(self.name, NodeEccentricity.COMMAND_TO_COUNT_NODES_IN_TEMPORARY_GRAPH, None, None, 'json', True)
        
        for result in results['results']['bindings']:
            retrievedCount = int(result['count']['value'])
        
        return retrievedCount
    
    def countNodes(self):
        results = self.connection.urika.query(self.name, NodeEccentricity.COMMAND_TO_COUNT_NODES, None, None, 'json', True)
        
        for result in results['results']['bindings']:
            retrievedCount = int(result['count']['value'])
        
        return retrievedCount
        