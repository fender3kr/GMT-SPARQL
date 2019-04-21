'''
Created on Jan 21, 2016

@author: phantom
'''
from operation.GraphOperation import GraphOperation

class ConnectedComponents(GraphOperation):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = '''
            CREATE GRAPH <http://workingGraph>;
                                          '''
    COMMAND_TO_FINALIZE_WORKING_GRAPH = '''
            DROP GRAPH <http://workingGraph>;
                                        '''
    COMMAND_TO_INSERT_NODES_TO_WORKING_GRAPH = '''
            INSERT { GRAPH <http://workingGraph> { ?node <temp:labels> ?node }}
            WHERE {
                ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor
            };
                                               '''
    COMMAND_TO_UPDATE_NODE_TO_MINIMAL_NODE = '''
            DELETE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o }}
            WHERE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o }};

            DELETE { GRAPH <http://workingGraph> { ?node <temp:labels> ?previous }}
            INSERT { GRAPH <http://workingGraph> { ?node <temp:labels> ?update ; <temp:counts> 1 }}
            WHERE {
                {
                    SELECT ?node (MIN(?label) AS ?update)
                    WHERE {
                        GRAPH <http://workingGraph> {
                            ?neighbor <temp:labels> ?label
                        } .
                        ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor
                    }
                    GROUP BY ?node
                } .
                GRAPH <http://workingGraph> {
                    ?node <temp:labels> ?previous
                }
                FILTER (STR(?previous) > STR(?update))
            };    
                                             '''
    COMMAND_TO_CHECK_CONVERGENCE = '''
            SELECT (COUNT(*) as ?changed)
            WHERE {
                GRAPH <http://workingGraph> { 
                    ?vertex <temp:counts> ?count
                }
            }
                                   '''
    COMMAND_TO_RETRIEVE_RESULT = '''
            SELECT * WHERE { 
                GRAPH <http://workingGraph> { 
                    ?node <temp:labels> ?label 
                }
            }
                                 '''

    def __init__(self, connection):
        GraphOperation.__init__(self, 'Connected Components', connection)
    
    def initialize(self):
        self.connection.urika.update(self.name, ConnectedComponents.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
        self.connection.urika.update(self.name, ConnectedComponents.COMMAND_TO_INSERT_NODES_TO_WORKING_GRAPH)
    
    def process(self):
        self.iteration = 0
        
        while True:
            self.iteration += 1
            self.connection.urika.update(self.name, ConnectedComponents.COMMAND_TO_UPDATE_NODE_TO_MINIMAL_NODE)
            if self.converges():
                break
    
    def store(self):
        print '\tNumber of Iteration: ' + str(self.iteration)
        
        results = self.connection.urika.query(self.name, ConnectedComponents.COMMAND_TO_RETRIEVE_RESULT, None, None, 'json', True)
        for result in results['results']['bindings']:
            self.writer.writeLine('Node: ' + result['node']['value'] + " [Label: " + result['label']['value'] + "]")

    def finalize(self):
        self.connection.urika.update(self.name, ConnectedComponents.COMMAND_TO_FINALIZE_WORKING_GRAPH)
    
    def converges(self):
        count = 0
        converged = False
        results = self.connection.urika.query(self.name, ConnectedComponents.COMMAND_TO_CHECK_CONVERGENCE, None, None, 'json', True)
        for result in results['results']['bindings']:
            count = int(result['changed']['value'])

        if count == 0:
            converged = True
       
        return converged 
    