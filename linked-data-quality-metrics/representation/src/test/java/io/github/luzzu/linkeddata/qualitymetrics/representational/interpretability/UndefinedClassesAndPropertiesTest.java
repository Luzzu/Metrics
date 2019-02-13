/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.interpretability;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;



/**
 * @author Jeremy Debattista
 * 
 * Test for the Undefined Classes and Properties Metric.
 * In the used dataset, there are 11 Undefined Classes,
 * 23 Undefined Properties and a total of 145 unique
 * classes and properties.
 * 
 */
public class UndefinedClassesAndPropertiesTest  extends Assert {

	TestLoader loader = new TestLoader();
	UndefinedClassesAndProperties metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("testdumps/eis.ttl");
//		loader.loadDataSet("/Users/jeremy/Downloads/lobid.org.nt");
		metric =  new UndefinedClassesAndProperties();
	}
	

	@Test
	public void undefinedClassesAndPropertiesTest() throws MetricProcessingException{
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		metric.metricValue();
		assertEquals(0.7491525, metric.metricValue(), 0.01);
		
//		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/undefined-classes-properties.ttl");
	}
	
}
