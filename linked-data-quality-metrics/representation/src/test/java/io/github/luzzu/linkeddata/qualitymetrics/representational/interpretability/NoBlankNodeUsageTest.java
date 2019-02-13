/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.interpretability;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;


/*
 * SPARQL Query used to get all blank nodes in dataset:
 * 
 * SELECT DISTINCT (COUNT(*) as ?count) WHERE {
 * 	?s ?p ?o .
 * 	FILTER (isBlank(?s) || isBlank(?o))
 * }
 * 
 * SPARQL Query used to get all Unique DLC not rdf:type
 * SELECT (COUNT(DISTINCT ?s ) AS ?count) { 
 * 	{ ?s ?p ?o  } 
 * 	UNION { ?o ?p ?s } 
 * 	FILTER(!isBlank(?s) && !isLiteral(?s) && (?p != rdf:type)) }         
 * }
 * 
 */

/**
 * @author Jeremy Debattista
 * 
 * Test for the No Blank Node Usage Metric.
 * In the used dataset, there are 2 Blank Nodes
 * and a total of 573 unique DLC 
 */
public class NoBlankNodeUsageTest extends Assert {

	TestLoader loader = new TestLoader();
	BlankNodeUsage metric;
	
	PipedRDFIterator<Triple> iter;
	@Before
	public void setUp(){
		loader.loadDataSet("testdumps/eis.ttl");
		metric = new BlankNodeUsage();
	}
	

	@Test
	public void noBlankNodesTest(){
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.99652173913, metric.metricValue(), 0.00001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/blank-nodes.ttl");

	}
	
}
