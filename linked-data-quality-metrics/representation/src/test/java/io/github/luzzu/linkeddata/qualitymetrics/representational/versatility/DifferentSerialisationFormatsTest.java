/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.versatility;

import java.io.IOException;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;
import io.github.luzzu.operations.properties.PropertyManager;

/*
 * SPARQL Query Used 
 * 
 * SELECT DISTINCT (COUNT(?o) AS ?featureCount) {
 * 	?s <http://rdfs.org/ns/void#feature> ?o .
 * }
 * 
 */

/**
 * @author Jeremy Debattista
 * 
 * Tests the Different Serialisation Formats metric.
 * The dataset being used for tests has 2 different
 * serialisation formats
 */
public class DifferentSerialisationFormatsTest extends Assert {
	
	TestLoader loader = new TestLoader();
	DifferentSerialisationFormatsCount metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("testdumps/eis.ttl");
//		loader.loadDataSet("/Users/jeremy/Downloads/ecowlim.tfri.gov.tw.nt");
		PropertyManager.getInstance().addToEnvironmentVars("baseURI", "http://eis.iai.uni-bonn.de/");
		PropertyManager.getInstance().addToEnvironmentVars("datasetURI", "http://eis.iai.uni-bonn.de/");
		metric = new DifferentSerialisationFormatsCount();
	}
	
	@Test
	public void differentSerilisationFormats() throws MetricProcessingException, IOException{
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
//		metric.metricValue();
		assertEquals(2.0, metric.metricValue(), 0.00001);
//		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/diff-serialisation-count.ttl");
	}
}
