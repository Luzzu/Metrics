/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.commons;

import io.github.luzzu.assessment.ComplexQualityMetric;

/**
 * @author Jeremy Debattista
 * 
 */
public abstract class AbstractComplexQualityMetric<T> implements ComplexQualityMetric<T> {

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
