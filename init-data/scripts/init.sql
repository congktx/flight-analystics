DROP SCHEMA datasource CASCADE;
CREATE SCHEMA datasource;

CREATE TABLE datasource.companies(
	company_id SERIAL PRIMARY KEY,
	company_cik VARCHAR(50),
	company_composite_figi VARCHAR(50),
	company_market_locale VARCHAR(50),
	company_share_class_figi VARCHAR(50),
	company_asset_type VARCHAR(50),
	company_ticker VARCHAR(50),
	company_name VARCHAR(50)
);

CREATE TABLE datasource.markets(
	market_id SERIAL PRIMARY KEY,
	market_type VARCHAR(50),
	market_region VARCHAR(50),
	market_local_close VARCHAR(50),
	market_local_open VARCHAR(50)
);

CREATE TABLE datasource.market_status(
	market_status_time_update INT4 NOT NULL,
	market_status_current_status VARCHAR(50),
	market_status_fk BIGINT NOT NULL REFERENCES datasource.markets(market_id),
	PRIMARY KEY (market_status_time_update, market_status_fk)
);



CREATE TABLE datasource.exchanges(
	exchange_id SERIAL PRIMARY KEY,
	exchange_mic VARCHAR(50),
	exchange_name VARCHAR(50),
	exchange_fk INT4 NOT NULL REFERENCES datasource.markets(market_id)
);

CREATE TABLE datasource.company_status(
	company_status_time_update INT4,
	company_status_type VARCHAR(50),
	company_status_active VARCHAR(50),
	company_status_fk INT4 NOT NULL REFERENCES datasource.companies(company_id),
	company_status_primary_exchange INT4 REFERENCES datasource.exchanges(exchange_id),
	PRIMARY KEY (company_status_fk, company_status_time_update)
);

COPY datasource.companies(company_id, company_cik, company_composite_figi,
                                                company_market_locale,
                                                company_share_class_figi,
                                                company_asset_type,
                                                company_ticker,
                                                company_name)
                        FROM '{GlobalConfig.COMPANIES_TABLE_PATH}'
                        DELIMITER ','
                        CSV HEADER;