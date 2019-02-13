/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.contextual.provenance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;

/**
 * @author Jeremy Debattista
 * 
 * This metric measures if a dataset have the most basic
 * provenance information, that is, information about the creator 
 * or publisher of the dataset. Each dataset (voID, dcat) should 
 * have either a dc:creator or dc:publisher as a minimum requirement. 
 * 
 */
public class BasicProvenanceMetric extends AbstractQualityMetric<Double> {
	
	protected ConcurrentHashMap<String, String> dataset = new ConcurrentHashMap<String,String>();
	protected Set<String> isMetadata = new HashSet<String>();

	private static Logger logger = LoggerFactory.getLogger(BasicProvenanceMetric.class);
	
	private long totalTriples = 0;

	@Override
	public void compute(Quad quad) {
		logger.debug("Assessing quad: " + quad.asTriple().toString());

		
		Node subject = quad.getSubject();
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();
		
		if (predicate.hasURI(RDF.type.getURI()) && (object.hasURI(VOID.Dataset.getURI()) || object.hasURI(DCAT.Dataset.getURI()))){
			if (subject.isURI()){
				dataset.put(subject.getURI(), "");
				isMetadata.add(subject.getURI());
			}
			else {
				dataset.put(subject.toString(), "");
				isMetadata.add(subject.toString());
			}
		}
		
		if (predicate.hasURI(DCTerms.creator.getURI()) || predicate.hasURI(DCTerms.publisher.getURI())){
			if (object.isURI()){
				if (subject.isURI()) {
					dataset.put(subject.getURI(), object.getURI());
				} else {
					dataset.put(subject.toString(), object.getURI());
				}
			}
		}
	}

	@Override
	public Double metricValue() {
		double validProv = 0.0;
		for (String s : dataset.values()) 
			if (!(s.equals(""))) validProv++;
		
		return (dataset.size() == 0) ? 0.0 : (validProv / (double)dataset.size());
	}

	@Override
	public Resource getMetricURI() {
		return DQM.BasicProvenanceMetric;
	}


	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}


	@Override
	public ProblemCollection<?> getProblemCollection() {
		return null;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral((int)this.totalTriples));
		
		return activity;
	}

}
