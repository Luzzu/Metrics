/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.contextual.understandability;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
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
 */
public class PresenceOfURIRegEx extends AbstractQualityMetric<Boolean> {

	final static Logger logger = LoggerFactory.getLogger(PresenceOfURIRegEx.class);

	private boolean uriRegExPresent = false;
	private long totalTriples = 0;

	
	@Override
	public void compute(Quad quad) {
		Node predicate = quad.getPredicate();
		totalTriples++;
		if (predicate.getURI().equals(VOID.uriRegexPattern.getURI())) {
			uriRegExPresent = true;
		}
	}

	@Override
	public Boolean metricValue() {
		return uriRegExPresent;
	}

	@Override
	public Resource getMetricURI() {
		return DQM.PresenceOfURIRegEx;
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
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.totalTriples));
		
		return activity;
	}

}
