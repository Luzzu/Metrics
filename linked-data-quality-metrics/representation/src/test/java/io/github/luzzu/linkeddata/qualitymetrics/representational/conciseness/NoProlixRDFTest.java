/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.conciseness;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;


/**
 * @author Jeremy Debattista
 * 
 * Test for the No Prolix RDF Test Metric.
 * In the used dataset, there are 29 RCC
 * and a total of 1475 triples 
 */
public class NoProlixRDFTest extends Assert {

	TestLoader loader = new TestLoader();
	NoProlixRDF metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("testdumps/eis.ttl");
//		loader.loadDataSet("/Users/jeremy/Downloads/lsdis.cs.uga.edu_projects_semdis_opus.nt.gz");
		 metric = new NoProlixRDF();
	}
	
	@Test
	public void noBlankNodesTest() throws MetricProcessingException{
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.98033898305, metric.metricValue(), 0.00001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/no-prolix-test.ttl");
	}
}