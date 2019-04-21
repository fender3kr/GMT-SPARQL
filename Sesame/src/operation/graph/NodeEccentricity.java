package operation.graph;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;

import exception.OperationException;
import operation.GraphOperation;

public class NodeEccentricity extends GraphOperation {
	private static final String NAME = "Node Eccentricity";
	private String node;
	
	private static final String QUERY_INIT_GRAPH = ""
			+ "CREATE GRAPH <http://workingGraph>;";
	private static final String QUERY_INIT_NODE = ""
			+ "INSERT { GRAPH <http://workingGraph> { %s <temp:labels> %d }}\n"
			+ "WHERE {};";
	private static final String QUERY_INSERT_NEIGHBORS = ""
			+ "INSERT { GRAPH <http://workingGraph> { ?neighbor <temp:labels> %d }}\n"
            + "WHERE {\n"
            + "  SELECT ?neighbor\n"
            + "  WHERE {\n"
            + "    {\n"
            + "      { ?node <urn:connectedTo> ?neighbor }\n"
            + "      UNION\n"
            + "      { ?neighbor <urn:connectedTo> ?node }\n"
            + "    } .\n"
            + "    { GRAPH <http://workingGraph> { ?node <temp:labels> %d }} .\n"
            + "    FILTER (!EXISTS { GRAPH <http://workingGraph> { ?neighbor <temp:labels> ?any }})\n"
            + "  }\n"
            + "};";  
	private static final String QUERY_COUNT_UPDATES = ""
			+ "SELECT (COUNT(*) AS ?count)\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> { ?s ?p ?o }\n"
			+ "}";
	private static final String QUERY_COUNT_NODES = ""
			+ "SELECT (COUNT(DISTINCT ?node) AS ?count)\n"
			+ "WHERE {\n"
			+ "  ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor"
			+ "}";
	private static final String QUERY_DROP_GRAPH = ""
			+ "DROP GRAPH <http://workingGraph>;";
	
	public NodeEccentricity(RepositoryConnection connection, String node) throws OperationException {
		super(NAME, connection);
		this.node = node;
	}
	
	@Override
	public void init() {
		connection.prepareUpdate(QUERY_INIT_GRAPH).execute();
		connection.prepareUpdate(String.format(QUERY_INIT_NODE, node, 0)).execute();
	}

	@Override
	public void process() {
		int iteration = 0;
		int previousCount = getUpdateCount();
		int currentCount = 0;
		boolean disjoint = false;
		
		while(true) {
			iteration++;
			connection.prepareUpdate(String.format(QUERY_INSERT_NEIGHBORS, iteration, iteration - 1)).execute();
			currentCount = getUpdateCount();
			if(previousCount == currentCount) {
				disjoint = isDisjoint(currentCount);
				break;	
			}
			
			previousCount = currentCount;
		}
		if(!disjoint)
			writer.writeLine("Eccentricity of " + node + " is " + (iteration - 1) + ".");
		else
			writer.writeLine("Eccentricity of " + node + " is infinite.");
	}

	@Override
	protected void end() {
		connection.prepareUpdate(QUERY_DROP_GRAPH).execute();
	}

	private int getUpdateCount() {
		int count = 0;
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_COUNT_UPDATES).evaluate();
		if(result.hasNext()) {
			BindingSet binding = result.next();
			count = ((Literal)binding.getValue("count")).intValue();
		}
		return count;
	}
	
	private int getNodeCount() {
		int count = 0;
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_COUNT_NODES).evaluate();
		if(result.hasNext()) {
			BindingSet binding = result.next();
			count = ((Literal)binding.getValue("count")).intValue();
		}
		
		return count;
	}
	
	private boolean isDisjoint(int count) {
		return count != getNodeCount();
	}
}
