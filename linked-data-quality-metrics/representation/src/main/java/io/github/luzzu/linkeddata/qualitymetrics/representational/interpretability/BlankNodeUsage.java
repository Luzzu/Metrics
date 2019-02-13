package io.github.luzzu.linkeddata.qualitymetrics.representational.interpretability;

import java.util.Set;
import java.util.UUID;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.mapdb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.mapdb.MapDbFactory;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;


/**
 * @author Jeremy Debattista
 * 
 * This metric calculates the number of blank nodes found in the subject or the object
 * of a triple. The metric value returns a value [0-1] where a higher number of blank nodes
 * will result in a value closer to 0.
 */
public class BlankNodeUsage extends AbstractQualityMetric<Double> {

	private static Logger logger = LoggerFactory.getLogger(BlankNodeUsage.class);
	
	//we will store all data level constraints that are URIs
	private static DB mapDb = MapDbFactory.getMapDBAsyncTempFile(BlankNodeUsage.class.getName());
	private Set<String> uniqueDLC = MapDbFactory.createHashSet(mapDb, UUID.randomUUID().toString());
	private Set<String> uniqueBN = MapDbFactory.createHashSet(mapDb, UUID.randomUUID().toString());
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.NoBlankNodeMetric);
	
//	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	private int tripleCount = 0;
	
	@Override
	public void compute(Quad quad) {
		Node predicate = quad.getPredicate();
		Node subject = quad.getSubject();
		Node object = quad.getObject();

		logger.debug("Assessing quad: " + quad.asTriple().toString());
		tripleCount++;

		// we will skip all "typed" triples
		if (!(predicate.hasURI(RDF.type.getURI()))){
			if (subject.isBlank()) {
				if (!(uniqueBN.contains(subject.getBlankNodeLabel()))) {
					uniqueBN.add(subject.getBlankNodeLabel());
//					if (requireProblemReport) problemCollection.addProblem(quad);
				}
			}
			else uniqueDLC.add(subject.getURI());
			
			if (!(object.isLiteral())){
				if (object.isBlank()){
					if (!(uniqueBN.contains(object.getBlankNodeLabel()))) {
						uniqueBN.add(object.getBlankNodeLabel());
//						if (requireProblemReport) problemCollection.addProblem(quad);
					}
				}
				else uniqueDLC.add(object.getURI());
			}
		}
	}

	@Override
	public Double metricValue() {
		double value = ((double) uniqueDLC.size()) / ((double) uniqueDLC.size() + (double) uniqueBN.size());
		// The value returned is the value of how much of the dataset is "blank node" free
		return value;
	}

	@Override
	public Resource getMetricURI() {
		return DQM.NoBlankNodeMetric;
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
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.tripleCount));
		
		return activity;
	}
}
