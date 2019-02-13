package io.github.luzzu.linkeddata.qualitymetics.commons;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.VocabularyLoader;
import io.github.luzzu.linkeddata.qualitymetrics.commons.exceptions.VocabularyUnreachableException;
import io.github.luzzu.semantics.vocabularies.DAQ;



/**
 * @author Jeremy Debattista
 * 
 */
public class VocabularyLoaderTest extends Assert {
	
	@Before
	public void setUp() throws Exception {
		VocabularyLoader.getInstance().clearDataset();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void knownVocabularyPropertiesTest() throws IOException, URISyntaxException, VocabularyUnreachableException {
		assertTrue(VocabularyLoader.getInstance().checkTerm(FOAF.page.asNode()));
		assertFalse(VocabularyLoader.getInstance().checkTerm(ModelFactory.createDefaultModel().createResource(FOAF.NS+"false").asNode()));
		assertTrue(VocabularyLoader.getInstance().checkTerm(RDF.li(1).asNode()));
		assertFalse(VocabularyLoader.getInstance().checkTerm(ModelFactory.createDefaultModel().createResource(FOAF.NS+"_1").asNode()));
	}

	@Test
	public void unknownVocabularyPropertiesTest() throws IOException, URISyntaxException, VocabularyUnreachableException  {
		assertTrue(VocabularyLoader.getInstance().checkTerm(DAQ.computedOn.asNode()));
		assertFalse(VocabularyLoader.getInstance().checkTerm(ModelFactory.createDefaultModel().createResource(DAQ.NS+"false").asNode()));
	}
	
	@Test
	public void knownVocabularyClassTest() throws IOException, URISyntaxException, VocabularyUnreachableException {
		assertTrue(VocabularyLoader.getInstance().checkTerm(FOAF.Agent.asNode()));
		assertFalse(VocabularyLoader.getInstance().checkTerm(ModelFactory.createDefaultModel().createResource(FOAF.NS+"False").asNode()));
	}

	@Test
	public void unknownVocabularyClassTest() throws IOException, URISyntaxException, VocabularyUnreachableException {
		assertTrue(VocabularyLoader.getInstance().checkTerm(DAQ.Category.asNode()));
		assertFalse(VocabularyLoader.getInstance().checkTerm(ModelFactory.createDefaultModel().createResource(DAQ.NS+"False").asNode()));
	}

}
