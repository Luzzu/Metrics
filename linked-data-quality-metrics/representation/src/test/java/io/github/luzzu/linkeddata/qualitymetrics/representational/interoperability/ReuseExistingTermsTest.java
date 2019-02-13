/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.interoperability;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.BeforeException;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;
import io.github.luzzu.operations.properties.PropertyManager;

/**
 * @author Jeremy Debattista
 * 
 * This tests the Reuse Existing Terms metric.
 * The dataset used has 2 distinct classes and
 * 14 distinct properties, of which 1 is a reused 
 * class term and 11 are reused property terms.
 */
public class ReuseExistingTermsTest extends Assert {

	TestLoader loader = new TestLoader();
	ReuseExistingTerms metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("testdumps/conf.rdf");
		
		PropertyManager.getInstance().addToEnvironmentVars("datasetURI", "http://colinda.org/resource/conference/");
		PropertyManager.getInstance().addToEnvironmentVars("baseURI", "http://colinda.org/");
		metric = new ReuseExistingTerms();

		try {
			String filePath = this.getClass().getClassLoader().getResource("config.ttl").getFile();
			Object[] before = new Object[1];
			before[0] = filePath;
			metric.before(before);
		} catch (BeforeException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void reuseExistingTerms() throws MetricProcessingException{
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.4375, metric.metricValue(), 0.15);
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/reuse-terms.ttl");
	}
}
