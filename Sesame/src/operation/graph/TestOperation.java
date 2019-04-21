package operation.graph;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;

import exception.OperationException;
import operation.GraphOperation;

public class TestOperation extends GraphOperation {
	private static final String NAME = "Test Operation";
	private String node;
	
	private static final String QUERY = ""
			+ "PREFIX geo: <http://geo.linkedopendata.gr/gag/ontology/>\n"
			+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
			+ "PREFIX units: <http://www.opengis.net/def/uom/OGC/1.0/>\n"
			+ "SELECT ?geometry1 ?geometry2 ?distance\n"
			+ "WHERE {\n"
			+ "  ?geometry1 geo:asWKT ?wkt1 .\n"
			+ "  ?geometry2 geo:asWKT ?wkt2 .\n"
			+ "  FILTER (STR(?geometry1) <= STR(?geometry2)) .\n"
			+ "  BIND (geof:distance(?wkt1, ?wkt2, units:metre) AS ?distance)"
			+ "}";
	
	public TestOperation(RepositoryConnection connection, String node) throws OperationException {
		super(NAME, connection);
		this.node = node;
	}
	 
	@Override
	public void init() {
	}

	@Override
	public void process() {
	}

	@Override
	protected void end() {
		TupleQueryResult result = connection.prepareTupleQuery(QUERY).evaluate();
		while(result.hasNext()) {
			BindingSet binding = result.next();
			String s1 = binding.getValue("geometry1").toString();
			String s2 = binding.getValue("geometry2").toString();
			String s3 = binding.getValue("distance").toString();
			System.out.println(s1 + " : " + s2 + " : " + s3);
		}
	}
}
