/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.contextual.provenance;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;


/**
 * @author Jeremy Debattista
 * 
 */
public class ExtendedProvenanceMetricTest extends Assert {

	TestLoader loader = new TestLoader();
	ExtendedProvenanceMetric metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("prov.ttl");
		metric = new ExtendedProvenanceMetric();
	}
	
	@Test
	public void basicProvInfoMetricTest(){
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.75, metric.metricValue(), 0.0001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Contextual/extended-prov.ttl");
	}
}
