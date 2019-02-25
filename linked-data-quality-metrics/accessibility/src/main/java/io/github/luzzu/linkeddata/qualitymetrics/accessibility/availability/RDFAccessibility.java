/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.accessibility.availability;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;


/**
 * @author Jeremy Debattista
 * 
 * Check if ALL data dumps (void:dataDump) exist, are reachable and parsable.
 *     
 */
public class RDFAccessibility extends AbstractQualityMetric<Boolean> {
	
	static Logger logger = LoggerFactory.getLogger(RDFAccessibility.class);
	
	private final Resource METRIC_URI = DQM.RDFAvailabilityMetric;
	
	private boolean hasRDFDump = false;
	
	

	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Computing : {} ", quad.asTriple().toString());
		
		if ((quad.getSubject().getURI().equals(EnvironmentProperties.getInstance().getDatasetPLD()))
				&& (quad.getPredicate().getURI().equals(VOID.dataDump.getURI()))) {
			this.hasRDFDump = true;
		}
	}

	@Override
	public Boolean metricValue() {
		return this.hasRDFDump;
	}


	public Resource getMetricURI() {
		return this.METRIC_URI;
	}
	
//	@Override
//	public ProblemList<?> getQualityProblems() {
//		ProblemList<Quad> tmpProblemList = null;
//		
//		if (this.metricValue() == 0){
//			String resource = this.getDatasetURI();
//			Resource subject = ModelFactory.createDefaultModel().createResource(resource);
//			Quad q = new Quad(null, subject.asNode() , QPRO.exceptionDescription.asNode(), DQMPROB.NoRDFAccessibility.asNode());
//			this.problemList.add(q);
//		}
//		
//		try {
//			if(this.problemList != null && this.problemList.size() > 0) {
//				tmpProblemList = new ProblemList<Quad>(this.problemList);
//			} else {
//				tmpProblemList = new ProblemList<Quad>();
//			}
//		} catch (ProblemListInitialisationException problemListInitialisationException) {
//			//TODO
//		}
//		return tmpProblemList;
//	}
	
	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return 	DQM.LuzzuProvenanceAgent;
	}

	

	@Override
	public ProblemCollection<?> getProblemCollection() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		return activity;
	}
	
}
