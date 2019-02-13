/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.syntacticvalidity;

import java.util.List;

import org.apache.jena.sparql.core.Quad;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;

/**
 * @author Jeremy Debattista
 * 
 */
public class CorrectLanguageTagTest extends Assert {

	protected TestLoader loader = new TestLoader();
	protected CorrectLanguageTag metric;

	
	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/www.wikidata.org.nt");
		metric = new CorrectLanguageTag();
		metric.before();
	}
	
	@After
	public void tearDown() throws Exception {
	}

	@Ignore
	@Test
	public void testCorrectLanguageTagTest() {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			// here we start streaming triples to the quality metric
			metric.compute(quad);
		}
		
		// total valid strings (i.e. with a language tag) 7
		// total correct 5
		// 5 / 7
		assertEquals(0.745,metric.metricValue(), 0.15);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/correct-language-tag.ttl");
	}	
	
}
