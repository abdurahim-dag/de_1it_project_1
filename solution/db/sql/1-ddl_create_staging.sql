create schema if not exists staging;

CREATE TABLE IF NOT EXISTS staging.stocks
(
    "time" timestamp with time zone NOT NULL,
    open numeric(10, 4),
    high numeric(10, 4),
    low numeric(10, 4),
    close numeric(10, 4),
    volume integer,
    upload_id bigint
);

CREATE TABLE IF NOT EXISTS staging.upload_hist
(
    upload_id bigserial,
    date timestamp with time zone NOT NULL,
    symbol_name character varying(50) NOT NULL,
    interval_name character varying(200) NOT NULL,
    uploaded boolean,
    PRIMARY KEY (upload_id)
);

ALTER TABLE IF EXISTS staging.stocks
    ADD CONSTRAINT stocks_upload_fk FOREIGN KEY (upload_id)
        REFERENCES staging.upload_hist (upload_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
