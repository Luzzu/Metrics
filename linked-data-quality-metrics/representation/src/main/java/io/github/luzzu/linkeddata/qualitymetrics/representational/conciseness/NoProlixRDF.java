/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.conciseness;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Jeremy Debattista
 * 
 * This metric detects the use of standard RDF Prolix Features.
 * These features are Collections (rdf:Alt, rdf:Bag, rdf:List, rdf:Seq), Containers and Reification (rdf:Statement).
 * 
 * The value returns a ratio of the total number of prolix (RCC) triples against the total number of triples
 */
public class NoProlixRDF extends AbstractQualityMetric<Double> {

	private double totalTriples = 0.0;
	
	private double totalRCC = 0.0;
	private static Logger logger = LoggerFactory.getLogger(NoProlixRDF.class);
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.NoProlixRDFMetric);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();
	
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing quad: " + quad.asTriple().toString());

		Node predicate = quad.getPredicate();
		Node object = quad.getObject();
		Node subject = quad.getSubject();
		
		totalTriples++;
		
		if (predicate.hasURI(RDF.type.getURI())){
			if (object.hasURI(RDF.Statement.getURI())){
				if (requireProblemReport) {
					Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.UsageOfReification.asNode());
					problemCollection.addProblem(q);
				}
				totalRCC++;
			} else if ((object.hasURI(RDFS.Container.getURI())) || object.hasURI(RDFS.ContainerMembershipProperty.getURI())) {
				if (requireProblemReport) {
					Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.UsageOfContainers.asNode());
					problemCollection.addProblem(q);
				}
				totalRCC++;
			} else if ( (object.hasURI(RDF.Alt.getURI())) || (object.hasURI(RDF.Bag.getURI())) || (object.hasURI(RDF.List.getURI())) || (object.hasURI(RDF.Seq.getURI()))){
				if (requireProblemReport) {
					Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.UsageOfCollections.asNode());
					problemCollection.addProblem(q);
				}
				totalRCC++;
			}
		} else {
			this.isRCCpredicate(subject, predicate);
		}
		

	}

	private void isRCCpredicate(Node subject, Node predicate){
		if ((predicate.hasURI(RDF.subject.getURI())) || (predicate.hasURI(RDF.predicate.getURI())) || (predicate.hasURI(RDF.object.getURI()))){
			if (requireProblemReport) {
				Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.UsageOfReification.asNode());
				problemCollection.addProblem(q);
			}
			totalRCC++;
		}
		if (predicate.hasURI(RDFS.member.getURI())){
			if (requireProblemReport) {
				Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.UsageOfContainers.asNode());
				problemCollection.addProblem(q);
			}
			totalRCC++;
		}
		if ((predicate.hasURI(RDF.first.getURI())) || (predicate.hasURI(RDF.rest.getURI())) || (predicate.hasURI(RDF.nil.getURI()))){
			if (requireProblemReport) {
				Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.UsageOfCollections.asNode());
				problemCollection.addProblem(q);
			}
			totalRCC++;
		}
		// for rdf:_n where n is a number
		if (predicate.getURI().matches(RDF.getURI()+"_[0-9]+")){
			if (requireProblemReport) {
				Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.UsageOfContainers.asNode());
				problemCollection.addProblem(q);
			}
			totalRCC++;
		}
	}

	
	@Override
	public Double metricValue() {
		return (this.totalRCC == 0) ? 1.0 : 1.0 - (this.totalRCC / this.totalTriples);
	}

	@Override
	public Resource getMetricURI() {
		return DQM.NoProlixRDFMetric;
	}

	

	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return 	DQM.LuzzuProvenanceAgent;
	}
	

	@Override
	public ProblemCollection<Quad> getProblemCollection() {
		return this.problemCollection;
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
