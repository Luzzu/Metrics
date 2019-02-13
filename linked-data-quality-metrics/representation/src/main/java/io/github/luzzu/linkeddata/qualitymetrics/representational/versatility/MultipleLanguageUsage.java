/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.versatility;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
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
 * In this metric we check if data (in this case literals) is
 * available in different languages, i.e. a dataset supports 
 * multilinguality. In this metric, we will check ALL literals
 * having a language tag. Those without a language tag will be
 * ignored.
 * 
 * The value of this metric is the average number of languages 
 * used throughout the dataset (per resource).
 * Therefore if we have the following:
 * R1 : [en,mt]
 * R2 : [en,mt]
 * R3 : [en]
 * R4 : [en]
 * R5 : [en]
 * the value would be 1 (7/5 = 1.4 ~ 1)
 * 
 * The value returns the number of multiple languages used
 */
public class MultipleLanguageUsage extends AbstractQualityMetric<Integer> {
	
	private static Logger logger = LoggerFactory.getLogger(MultipleLanguageUsage.class);
	
//	static final String DEFAULT_TAG = "en";
	
	private ConcurrentHashMap<String, Set<String>> multipleLanguage = new ConcurrentHashMap<String, Set<String>>();
//	private ConcurrentHashMap<Integer, Integer> languagePerResource = new ConcurrentHashMap<Integer, Integer>();
	
	private int tripleCount = 0;
	
	@Override
	public void compute(Quad quad) {
		logger.debug("Assessing {}",quad.asTriple().toString());
		Node object = quad.getObject();
		
		tripleCount++;
		
		if (object.isLiteral()){
			String subject = quad.getSubject().toString();
			String lang = object.getLiteralLanguage();
			if (!(lang.equals(""))){
				Set<String> langList = new HashSet<String>();
				if (this.multipleLanguage.containsKey(subject)) langList = this.multipleLanguage.get(subject);
				langList.add(lang);
				this.multipleLanguage.put(subject,langList);
			}
		}
	}

	@Override
	public Integer metricValue() {
		double totLang = 0.0;
		
		
		for(Set<String> lst : this.multipleLanguage.values()) {
			totLang += (double) lst.size();
//			if (languagePerResource.containsKey(lst.size())) languagePerResource.compute(lst.size(), (a,b) -> b + 1);
//			else languagePerResource.put(lst.size(), 1);
		}
		double val = totLang / (double) this.multipleLanguage.size();
		
		return (Math.round(val) == 0) ? 1 : (int) Math.round(val) ;
	}

	@Override
	public Resource getMetricURI() {
		return DQM.MultipleLanguageUsageMetric;
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
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.tripleCount));
		
		return activity;
	}


}
