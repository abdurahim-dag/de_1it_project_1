create schema if not exists mart;

CREATE TABLE IF NOT EXISTS mart.d_symbol_act
(
    symbol_id bigserial NOT NULL,
    symbol_name character varying(50) NOT NULL,
    CONSTRAINT symbol_pk PRIMARY KEY (symbol_id)
);

CREATE TABLE IF NOT EXISTS mart.d_symbol_hist
(
    symbol_id bigint NOT NULL,
    symbol_valid_from timestamp with time zone NOT NULL,
    symbol_valid_to timestamp with time zone,
    symbol_name character varying(50),
    CONSTRAINT symbol_hist_pk PRIMARY KEY (symbol_id, symbol_valid_from)
);

CREATE TABLE IF NOT EXISTS mart.f_price
(
    price_id bigserial,
    price_open numeric(10, 4),
    price_high numeric(10, 4),
    price_low numeric(10, 4),
    price_close numeric(10, 4),
    price_volume integer,
    price_symbol_id bigint NOT NULL,
    price_time_id bigint NOT NULL,
    CONSTRAINT price_pk PRIMARY KEY (price_id)
);

CREATE TABLE IF NOT EXISTS mart.d_time_serial
(
    time_serial_id bigserial,
    time_serial_time timestamp with time zone NOT NULL,
    time_serial_interval_id bigint NOT NULL,
    CONSTRAINT time_serial_pk PRIMARY KEY (time_serial_id)
);

CREATE TABLE IF NOT EXISTS mart.d_interval
(
    interval_id bigserial,
    interval_name character varying(200) NOT NULL,
    CONSTRAINT interval_pk PRIMARY KEY (interval_id)
);

ALTER TABLE IF EXISTS mart.f_price
    ADD CONSTRAINT price_time_serial_fk FOREIGN KEY (price_time_id)
        REFERENCES mart.d_time_serial (time_serial_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;


ALTER TABLE IF EXISTS mart.f_price
    ADD CONSTRAINT price_symbol_fk FOREIGN KEY (price_symbol_id)
        REFERENCES mart.d_symbol_act (symbol_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;


ALTER TABLE IF EXISTS mart.d_time_serial
    ADD CONSTRAINT time_serial_interval_fk FOREIGN KEY (time_serial_interval_id)
        REFERENCES mart.d_interval (interval_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
