/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.versatility;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;

/**
 * @author Jeremy Debattista
 * 
 * Test for the Multiple Language Usage Metric
 * 
 */
public class MultipleLanguageUsageTest  extends Assert {

	TestLoader loader = new TestLoader();
	MultipleLanguageUsage metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("testdumps/language.ttl");
		metric = new MultipleLanguageUsage();

	}
	
	@Test
	public void multipleLanguagesTest(){
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(3.0, metric.metricValue(), 0.00001);
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/multiple-languages.ttl");

	}
}
