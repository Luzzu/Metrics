/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.commons;

import io.github.luzzu.assessment.QualityMetric;

/**
 * @author Jeremy Debattista
 * 
 */
public abstract class AbstractQualityMetric<T> implements QualityMetric<T> {

	private String datasetURI = "";
	
	@Override
	public String getDatasetURI() {
		return this.datasetURI;
	}

	@Override
	public void setDatasetURI(String datasetURI) {
		this.datasetURI = datasetURI;
	}
}
