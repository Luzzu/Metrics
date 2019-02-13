package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.VocabularyLoader;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionModel;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;

/**
 * The Ontology Hijacking detects the redefinition by analyzing defined classes or 
 * properties in data set and looks of same definition in its respective vocabulary. 
 * 
 * This metric uses table 1 from http://www.aidanhogan.com/docs/saor_aswc08.pdf
 * in order to identify the rules for Ontology Hijacking.
 * 
 * @author Jeremy Debattista
 * 
 */
public class OntologyHijacking extends AbstractQualityMetric<Double> {

	private final Resource METRIC_URI = DQM.OntologyHijackingMetric;
	static Logger logger = LoggerFactory.getLogger(OntologyHijacking.class);

	private double totalPossibleHijacks = 0; // total number of redefined classes or properties
	private double totalHijacks = 0;

	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(DQM.OntologyHijackingMetric);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();
	
	private String dataset_pld = EnvironmentProperties.getInstance().getDatasetPLD();


	private List<HijackingRule> hijackingRules = new CustomList<HijackingRule>();
	{
		hijackingRules.add(new HijackingRule(RDFS.subClassOf, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.equivalentClass, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.equivalentClass, TriplePosition.OBJECT));
		hijackingRules.add(new HijackingRule(RDFS.subPropertyOf, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.equivalentProperty, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.equivalentProperty, TriplePosition.OBJECT));
		hijackingRules.add(new HijackingRule(OWL.inverseOf, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.inverseOf, TriplePosition.OBJECT));
		hijackingRules.add(new HijackingRule(RDFS.domain, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(RDFS.range, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.SymmetricProperty, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.onProperty, TriplePosition.OBJECT));
		hijackingRules.add(new HijackingRule(OWL.hasValue, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.unionOf, TriplePosition.OBJECT));
		hijackingRules.add(new HijackingRule(OWL.intersectionOf, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.intersectionOf, TriplePosition.OBJECT));
		hijackingRules.add(new HijackingRule(OWL.FunctionalProperty, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.InverseFunctionalProperty, TriplePosition.SUBJECT));
		hijackingRules.add(new HijackingRule(OWL.TransitiveProperty, TriplePosition.SUBJECT));
	}


	@SuppressWarnings("unlikely-arg-type")
	public void compute(Quad quad) throws MetricProcessingException {
		if (!quad.getObject().isURI()) return; //we do not need to test this

		Resource subject = ResourceCommons.asRDFNode(quad.getSubject()).asResource();
		Resource predicate = ResourceCommons.asRDFNode(quad.getPredicate()).asResource();
		Resource object = ResourceCommons.asRDFNode(quad.getObject()).asResource();

		// class type hijacking
		if (predicate.equals(RDF.type)){
			if (hijackingRules.contains(object)){
				// we triggered a hijacking rule
				
				if (!isAuthorative(subject)) {
					totalHijacks++;
					this.addToProblem(quad, hijackingRules.get(hijackingRules.indexOf(object)));
				}
				totalPossibleHijacks++;
			}
		} else if (hijackingRules.contains(predicate)){
			// property type hijacking - we might have multiple rules here
			List<HijackingRule> rules = new ArrayList<HijackingRule>();
			for (HijackingRule rule : hijackingRules){
				if (rule.equals(predicate)) rules.add(rule);
			}

			for (HijackingRule r : rules){
				Resource authoritativeCheck;
				if (r.authorativeSource == TriplePosition.SUBJECT) authoritativeCheck = subject;
				else authoritativeCheck = object;

				if (!isAuthorative(authoritativeCheck)) {
					totalHijacks++;
					this.addToProblem(quad, r);
				}
			}
			totalPossibleHijacks++;

		}
	}


	private void addToProblem(Quad q, HijackingRule rule){
		if (requireProblemReport) {
			Model m = ModelFactory.createDefaultModel();

			Resource gen = ResourceCommons.generateURI();
			m.add(gen, RDF.type, DQMPROB.OntologyHijackingException);

			if (rule.authorativeSource == TriplePosition.OBJECT) {
				m.add(gen, DQMPROB.hijackedConcept, ResourceCommons.asRDFNode(q.getObject()));
				m.add(gen, DQMPROB.hijackedBy, ResourceCommons.asRDFNode(q.getSubject()));
				m.add(gen, DQMPROB.hijackedRule, rule.hijackProperty);
			} else {
				m.add(gen, DQMPROB.hijackedConcept, ResourceCommons.asRDFNode(q.getSubject())); // the authorative sources is being hijacked
				m.add(gen, DQMPROB.hijackedBy, ResourceCommons.asRDFNode(q.getObject()));
				m.add(gen, DQMPROB.hijackedRule, rule.hijackProperty);
			}
			
//			problemCollection.addProblem(m);
			((ProblemCollectionModel)problemCollection).addProblem(m, gen);
		}
	}

	/**
	 * @param Concept being check for authority
	 * @return true if the assessed dataset is authorative to the concept
	 */
	private boolean isAuthorative(Resource node){
		/* A source s is authorative of concept c if:
		 *   1) c is a blank node OR
		 *   2) s is retrievable and is part of the namespace identifying c - given that c exists.
		 */

		if (node.isAnon()) return true;

		if (node.getNameSpace().contains(dataset_pld)) 
			return true;
		else 
			return !(VocabularyLoader.getInstance().checkTerm(node.asNode()));
	}


	/**
	 * Returns metric value for between 0 to 1. Where 1 as the best case and 0 as worst case 
	 * @return double - range [0 - 1] 
	 */

	public Double metricValue() {
		if (this.totalPossibleHijacks == 0) return 1.0;

		double value = Math.abs(1.0 - (this.totalHijacks / this.totalPossibleHijacks));

		return value;
	}


	public Resource getMetricURI() {
		return this.METRIC_URI;
	}


	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}

	private class HijackingRule{
		Resource hijackProperty; // could be a property or resource
		TriplePosition authorativeSource;

		HijackingRule(Resource resource, TriplePosition pos){
			hijackProperty = resource;
			authorativeSource = pos;
		}

		@Override
		public boolean equals(Object object){
			boolean sameSame = false;

			if (object != null && object instanceof Resource){
				sameSame = this.hijackProperty.equals(((Resource) object));
			}

			return sameSame;
		}
	}

	private enum TriplePosition{
		SUBJECT, PREDICATE, OBJECT;
	}

	private class CustomList<T> extends ArrayList<T>{
		private static final long serialVersionUID = 1L;

		@Override
		public int indexOf(Object o) {
			if (o == null) {
				for (int i = 0; i < this.size(); i++)
					if (this.get(i)==null)return i;
			} else {
				for (int i = 0; i < this.size(); i++)
					if (this.get(i).equals(o))
						return i;
			}
			return -1;
		}
	}

	@Override
	public ProblemCollection<?> getProblemCollection() {
		return this.problemCollection;
	}


	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		
		return activity;	
	}

}
