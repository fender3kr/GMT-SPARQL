package operation.graph;

import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;

import exception.OperationException;
import operation.GraphOperation;

public class PageRank extends GraphOperation {
	private static final String NAME = "PageRank";
	
	private static final String QUERY_INIT_GRAPH = ""
			+ "CREATE GRAPH <http://workingGraph>;";
	private static final String QUERY_NODE_COUNT = ""
			+ "SELECT (COUNT(DISTINCT ?node) AS ?count)\n"
			+ "WHERE {\n"
			+ "  ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor\n"
			+ "}";
	private static final String QUERY_OUTDEGREES = ""
			+ "INSERT { GRAPH <http://workingGraph> { ?node <temp:outDegree> ?outDegree }}\n"
			+ "WHERE {\n"
			+ "  {\n"
			+ "    SELECT ?node (COUNT(*) AS ?outDegree)\n"
			+ "    WHERE {\n"
			+ "      ?node <urn:connectedTo> ?neighbor\n"
			+ "    }\n"
			+ "    GROUP BY ?node\n"
			+ "  }\n"
			+ "  UNION\n"
			+ "  {\n"
			+ "    SELECT ?node ?outDegree\n"
			+ "    WHERE {\n"
			+ "      ?neighbor <urn:connectedTo> ?node .\n"
			+ "      OPTIONAL { ?node <urn:connectedTo> ?any } .\n"
			+ "      FILTER (!BOUND(?any)) .\n"
			+ "      BIND (0 AS ?outDegree)\n"
			+ "    }\n"
			+ "  }\n"
			+ "};";
	private static final String QUERY_INITIAL_PAGERANKS = ""
			+ "INSERT { GRAPH <http://workingGraph> { ?node <temp:rank> %f }}\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?node <temp:outDegree> []\n"
			+ "  }\n"
			+ "};";
	private static final String QUERY_UPDATE_PAGERANKS = ""
			+ "DELETE { GRAPH <http://workingGraph> { ?node <temp:rank> ?previousRank }}\n"
			+ "INSERT { GRAPH <http://workingGraph> { ?node <temp:rank> ?newRank; <temp:delta> ?delta }}\n"
			+ "WHERE {\n"
			+ "  {\n"
			+ "    SELECT ?node (SUM(?score) * %f + (1.0 - %f) / %d AS ?newRank)\n"
			+ "    WHERE {\n"
			+ "      ?neighbor <urn:connectedTo> ?node .\n"
			+ "      GRAPH <http://workingGraph> {\n"
			+ "        ?neighbor <temp:rank> ?rank; <temp:outDegree> ?outDegree .\n"
			+ "        BIND ((?rank / ?outDegree) AS ?score)\n"
			+ "      }\n"
			+ "    }\n"
			+ "    GROUP BY ?node\n"
			+ "  } .\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?node <temp:rank> ?previousRank\n"
			+ "  } .\n"
			+ "  BIND (ABS(?previousRank - ?newRank) AS ?delta)\n"
			+ "};";
	private static final String QUERY_EPSILON = ""
			+ "SELECT (SUM(?delta) AS ?epsilon)\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?node <temp:delta> ?delta\n"
			+ "  }\n"
			+ "}";
	private static final String QUERY_DELETE_EPSILON = ""
			+ "DELETE { GRAPH <http://workingGraph> { ?node <temp:delta> ?delta }}\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?node <temp:delta> ?delta\n"
			+ "  }\n"
			+ "};";
	private static final String QUERY_RETRIEVE_PAGERANKS = ""
			+ "SELECT ?node ?rank\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?node <temp:rank> ?rank\n"
			+ "  }\n"
			+ "}";
	private static final String QUERY_DROP_GRAPH = ""
			+ "DROP GRAPH <http://workingGraph>;";
	
	private int numberOfNodes;
	private float dampingFactor;
	private float epsilon;
	
	public PageRank(RepositoryConnection connection, float epsilon, float dampingFactor) throws OperationException {
		super(NAME, connection);
		this.dampingFactor = dampingFactor;
		this.epsilon = epsilon;
	}
	
	@Override
	protected void init() {
		numberOfNodes = computeNumberOfNodes();
		connection.prepareUpdate(QUERY_INIT_GRAPH).execute();
		connection.prepareUpdate(QUERY_OUTDEGREES).execute();
		connection.prepareUpdate(String.format(QUERY_INITIAL_PAGERANKS, (1.0f / numberOfNodes))).execute();
	}

	@Override
	protected void process() {
		int iteration = 0;
		
		do {
			iteration++;
			System.out.println("\tIteration: " + iteration);
			connection.prepareUpdate(String.format(QUERY_UPDATE_PAGERANKS, this.dampingFactor, this.dampingFactor, this.numberOfNodes)).execute();
		} while (!converges());
		
		System.out.println("The number of iterations: " + iteration);
		savePageRanks();
	}

	@Override
	protected void end() {
		connection.prepareUpdate(QUERY_DROP_GRAPH).execute();
	}

	private int computeNumberOfNodes() {
		int count = 0;
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_NODE_COUNT).evaluate();
		if(result.hasNext()) {
			BindingSet binding = result.next();
			count = ((Literal)binding.getValue("count")).intValue();
		}
		return count;
	}
	
	private boolean converges() {
		float epsilon = 0.0f;
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_EPSILON).evaluate();
		if(result.hasNext()) {
			BindingSet binding = result.next();
			epsilon = ((Literal)binding.getValue("epsilon")).floatValue();
		}
		System.out.println("\tEpsilon: " + epsilon);
		connection.prepareUpdate(QUERY_DELETE_EPSILON).execute();
		return epsilon < this.epsilon;
	}
	
	private void savePageRanks() {
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_RETRIEVE_PAGERANKS).evaluate();
		while(result.hasNext()) {
			BindingSet binding = result.next();
			String node = binding.getValue("node").toString();
			float rank = ((Literal)binding.getValue("rank")).floatValue();
			
			writer.writeLine("Node: " + node + " [PageRank: " + (String.format("%.7f", rank)) + "]");
		}
	}
}
