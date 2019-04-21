'''
Created on Jan 21, 2016

@author: phantom
'''
from operation.GraphOperation import GraphOperation

class PageRank(GraphOperation):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = '''
            CREATE GRAPH <http://workingGraph>;
                                          '''
    COMMAND_TO_FINALIZE_WORKING_GRAPH = '''
            DROP GRAPH <http://workingGraph>;
                                        '''
    COMMAND_TO_COUNT_NODES = '''
            SELECT (COUNT(DISTINCT ?node) AS ?count)
            WHERE {
                ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor
            }
                             '''
    COMMAND_TO_RETRIEVE_OUTDEGREE = '''
            INSERT { GRAPH <http://workingGraph> { ?node <temp:outDegree> ?outDegree }}
            WHERE {
                {
                    SELECT ?node (COUNT(*) AS ?outDegree)
                    WHERE {
                        ?node <urn:connectedTo> ?neighbor
                    }
                    GROUP BY ?node
                }
                UNION
                {
                    SELECT ?node ?outDegree
                    WHERE {
                        ?neighbor <urn:connectedTo> ?node .
                        OPTIONAL { ?node <urn:connectedTo> ?any } .
                        FILTER (!BOUND(?any)) .
                        BIND (0 AS ?outDegree)
                    }
                }
            };
                                    '''
    COMMAND_TO_SET_INITIAL_PAGERANKS = '''
            INSERT {{ GRAPH <http://workingGraph> {{ ?node <temp:rank> {RANK} }}}}
            WHERE {{
                GRAPH <http://workingGraph> {{
                    ?node <temp:outDegree> []
                }}
            }};
                                       '''
    COMMAND_TO_UPDATE_PAGERANKS = '''
            DELETE {{ GRAPH <http://workingGraph> {{ ?node <temp:rank> ?previousRank }}}}
            INSERT {{ GRAPH <http://workingGraph> {{ ?node <temp:rank> ?newRank; <temp:delta> ?delta }}}}
            WHERE {{
                {{
                    SELECT ?node (SUM(?score) * {DAMPING_FACTOR} + (1.0 - {DAMPING_FACTOR}) / {NODES} AS ?newRank)
                    WHERE {{
                        GRAPH <http://workingGraph> {{
                            ?neighbor <temp:rank> ?rank; <temp:outDegree> ?outDegree .
                            BIND ((?rank / ?outDegree) AS ?score)
                        }} .
                        ?neighbor <urn:connectedTo> ?node .
                    }}
                    GROUP BY ?node
                }} .
                GRAPH <http://workingGraph> {{
                    ?node <temp:rank> ?previousRank
                }} .
                BIND (ABS(?previousRank - ?newRank) AS ?delta)
            }};
                                  '''
    COMMAND_TO_CHECK_CONVERGENCE = '''
            SELECT (SUM(?delta) AS ?epsilon)
            WHERE {
                GRAPH <http://workingGraph> {
                    ?node <temp:delta> ?delta
                }
            }
                                   '''
    COMMAND_TO_DELETE_DELTAS = '''
            DELETE { GRAPH <http://workingGraph> { ?node <temp:delta> ?delta }}
            WHERE {
                GRAPH <http://workingGraph> {
                    ?node <temp:delta> ?delta
                }
            };
                               '''
    COMMAND_TO_RETRIEVE_PAGERANKS = '''
            SELECT ?node ?rank
            WHERE {
                GRAPH <http://workingGraph> {
                    ?node <temp:rank> ?rank
                }
            }
                                    '''

    def __init__(self, epsilon, dampingFactor, connection):
        GraphOperation.__init__(self, 'PageRank', connection)
        self.epsilon = epsilon
        self.dampingFactor = dampingFactor
    
    def initialize(self):
        self.connection.urika.update(self.name, PageRank.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
        self.connection.urika.update(self.name, PageRank.COMMAND_TO_RETRIEVE_OUTDEGREE)
        results = self.connection.urika.query(self.name, PageRank.COMMAND_TO_COUNT_NODES, None, None, 'json', True)
        for result in results['results']['bindings']:
            self.numberOfNodes = int(result['count']['value'])
        
        self.connection.urika.update(self.name, PageRank.COMMAND_TO_SET_INITIAL_PAGERANKS.format(RANK = (1.0 / self.numberOfNodes)))
    
    def process(self):
        self.iteration = 0
        
        while True:
            self.iteration += 1
            print '\tIteration: ' + str(self.iteration)
            
            self.connection.urika.update(self.name, PageRank.COMMAND_TO_UPDATE_PAGERANKS.format(DAMPING_FACTOR = self.dampingFactor, NODES = self.numberOfNodes))
            
            if self.converges():
                break
            
    def store(self):
        print '\tNumber of Iteration: ' + str(self.iteration)
        
        results = self.connection.urika.query(self.name, PageRank.COMMAND_TO_RETRIEVE_PAGERANKS)
        for result in results['results']['bindings']:
            self.writer.writeLine('Node: ' + result['node']['value'] + " [PageRank: " + result['rank']['value'] + "]")
    
    def finalize(self):
        self.connection.urika.update(self.name, PageRank.COMMAND_TO_FINALIZE_WORKING_GRAPH)
    
    def converges(self):
        results = self.connection.urika.query(self.name, PageRank.COMMAND_TO_CHECK_CONVERGENCE)
        for result in results['results']['bindings']:
            epsilon = float(result['epsilon']['value'])
        
        self.connection.urika.update(self.name, PageRank.COMMAND_TO_DELETE_DELTAS)
        
        return epsilon < self.epsilon