package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.List;

import org.apache.jena.sparql.core.Quad;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;


public class EntitiesAsMembersOfDisjointClassesTest extends Assert{
	
	protected TestLoader loader = new TestLoader();
	protected AdvancedEntitiesAsMembersOfDisjointClasses metric;

	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/SampleInput_EntitiesAsMembersOfDisjointClasses_Minimal.ttl");
		metric = new AdvancedEntitiesAsMembersOfDisjointClasses();
	}

	@After
	public void tearDown() throws Exception {
	}

	/**
	 * a minimal example for entities being members of disjoint classes: one entity is (of foaf:Person and foaf:Document), one isn't.
	 * 
	 * Note that the FOAF vocabulary has been published as LOD, and that foaf:Person is explicitly declared disjoint with foaf:Document.
	 */
	@Test
	public void testEntitiesAsMembersOfDisjointClassesMinimalExample() {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			metric.compute(quad);
		}
		
		// 1 / 2
		assertEquals(0.5,metric.metricValue(), 0.0001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/adv-entities-disjoint.ttl");

	}	
	

}