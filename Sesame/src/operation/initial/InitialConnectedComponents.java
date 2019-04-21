package operation.initial;

import operation.GraphOperation;

import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;

import exception.OperationException;

public class InitialConnectedComponents extends GraphOperation {
	private static final String NAME = "Initial Connected Components";
	
	private static final String QUERY_INIT_GRAPH = ""
			+ "CREATE GRAPH <http://workingGraph>;";
	private static final String QUERY_INIT_NODES = ""
			+ "INSERT { GRAPH <http://workingGraph> { ?node <temp:labels> ?node }}\n"
			+ "WHERE {\n"
			+ "  ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor\n"
			+ "};";
	private static final String QUERY_UPDATE_LABELS = ""
			+ "DELETE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o }}\n"
			+ "WHERE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o }};\n"
			+ "DELETE { GRAPH <http://workingGraph> { ?node <temp:labels> ?previous }}\n"
			+ "INSERT { GRAPH <http://workingGraph> { ?node <temp:labels> ?update ; <temp:counts> 1 }}\n"
			+ "WHERE {\n"
			+ "  {\n"
			+ "    SELECT ?node (MIN(?label) AS ?update)\n"
			+ "    WHERE {\n"
			+ "      ?node <urn:connectedTo>|^<urn:connectedTo> ?neighbor .\n"
			+ "      GRAPH <http://workingGraph> {\n"
			+ "        ?neighbor <temp:labels> ?label\n"
			+ "      }\n"
			+ "    }\n"
			+ "    GROUP BY ?node\n"
			+ "  } .\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?node <temp:labels> ?previous\n"
            + "  }\n"
			+ "  FILTER (STR(?previous) > STR(?update))\n"
            + "};";      
	private static final String QUERY_COUNT_UPDATES = ""
			+ "SELECT (COUNT(*) as ?changed)\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?vertex <temp:counts> ?count\n"
			+ "  }\n"
			+ "}";
	private static final String QUERY_RETRIEVE_RESULT = ""
			+ "SELECT ?node ?label\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?node <temp:labels> ?label\n"
			+ "  }\n"
			+ "}";
	private static final String QUERY_DROP_GRAPH = ""
			+ "DROP GRAPH <http://workingGraph>;";
	
	public InitialConnectedComponents(RepositoryConnection connection) throws OperationException {
		super(NAME, connection);
	}
	
	@Override
	protected void init() {
		connection.prepareUpdate(QUERY_INIT_GRAPH).execute();
		connection.prepareUpdate(QUERY_INIT_NODES).execute();
	}

	@Override
	protected void process() {
		int iteration = 0;
		
		while(true) {
			iteration++;
			System.out.println("\tIteration: " + iteration);
			connection.prepareUpdate(QUERY_UPDATE_LABELS).execute();
			if(converges())
				break;
		}
		saveComponents();
		System.out.println("The number of iterations: " + iteration);
	}

	@Override
	protected void end() {
		connection.prepareUpdate(QUERY_DROP_GRAPH).execute();
	}

	private boolean converges() {
		int count = 0;
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_COUNT_UPDATES).evaluate();
		if(result.hasNext()) {
			BindingSet binding = result.next();
			count = ((Literal)binding.getValue("changed")).intValue();
		}
		return count == 0;
	}
	
	private void saveComponents() {
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_RETRIEVE_RESULT).evaluate();
		while(result.hasNext()) {
			BindingSet binding = result.next();
			String node = binding.getValue("node").toString();
			String label = binding.getValue("label").toString();
			
			writer.writeLine("Node: " + node + " [Label: " + label + "]");
		}
	}
}
