/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.contextual.understandability;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

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
 * This metric checks whether vocabularies used in the datasets (ie. predicate or object if predicate is rdf:type) 
 * are indicated in the dataset's metadata, specifically using the void:vocabulary predicate 
 * 
 */
public class VocabularyUsageIndication extends AbstractQualityMetric<Double> {

	final static Logger logger = LoggerFactory.getLogger(VocabularyUsageIndication.class);

	private Set<String> differentNamespacesUsed = new HashSet<String>();
	private Set<String> namespacesIndicated = new HashSet<String>();

	private boolean calculated = false;
	private double value  = 0.0d;
	
	private long totalTriples = 0;

	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.HumanReadableLabellingMetric);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	private Set<String> nsIgnore = new HashSet<String>();
	{
		nsIgnore.add("http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		nsIgnore.add("http://www.w3.org/2002/07/owl#");
		nsIgnore.add("http://rdfs.org/ns/void#");
		nsIgnore.add("http://www.w3.org/2000/01/rdf-schema#");
	}
	
	
	@Override
	public void compute(Quad quad) {
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();
		totalTriples++;
		
		differentNamespacesUsed.add(predicate.getNameSpace());
		if (predicate.getURI().equals(RDF.type.getURI())){
			if (object.isURI()) differentNamespacesUsed.add(object.getNameSpace());
		}
		
		if (predicate.getURI().equals(VOID.vocabulary.getURI())) namespacesIndicated.add(object.getURI());
	}

	@Override
	public Double metricValue() {
		if (!calculated){
			calculated = true;
			
			double totalDiffNs = differentNamespacesUsed.size();
			double totalNsInd = namespacesIndicated.size();
			
			SetView<String> view = Sets.intersection(differentNamespacesUsed, namespacesIndicated); // view of indicated and used
			
			
			statsLogger.info("Dataset: {} - Total # NS used : {}; # NS indicated by void : {} # NS used vis-a-vie indicated : {};"
					, this.getDatasetURI(), totalDiffNs, totalNsInd, view.size()); //TODO: these store in a seperate file

		
			if (view.size() == 0) value = 0.0;
			else if (totalDiffNs == 0) value = 0.0;
			else value = (double)view.size()/(double)totalDiffNs;
		}
		return value;
	}

	@Override
	public Resource getMetricURI() {
		return DQM.VocabularyUsageIndication;
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
		
		if (problemCollection.isEmpty()) {
			if(requireProblemReport) {
				differentNamespacesUsed.removeAll(namespacesIndicated);
				for(String s : differentNamespacesUsed) {
					Quad q = new Quad(null, ModelFactory.createDefaultModel().createResource(s).asNode(), QPRO.exceptionDescription.asNode(), DQMPROB.NoVocabularyIndication.asNode());
					problemCollection.addProblem(q);
				}
			}
		}
		
		return this.problemCollection;
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
