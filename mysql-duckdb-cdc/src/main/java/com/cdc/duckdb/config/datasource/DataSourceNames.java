package com.cdc.duckdb.config.datasource;

public interface DataSourceNames {
	@Deprecated
	String CDS_MYSQL= "cds-mysql";
	@Deprecated
	String CDS_SLAVE_MYSQL= "cds-slave-mysql";

	String CEM_MASTER_MYSQL = "cem-master-mysql";
	String CEM_SLAVE_MYSQL = "cem-slave-mysql";

	String PAYMENT_MYSQL= "payment-master-mysql";
	String PAYMENT_SLAVE_MYSQL= "payment-slave-mysql";

	String STATISTICAL_ANALYSIS_DUCK = "statistical-analysis-duck";
}

