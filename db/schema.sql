-- Updated schema.sql for the provided data structure
CREATE TABLE IF NOT EXISTS distributors
(
    id               SERIAL PRIMARY KEY,
    distributor_name VARCHAR(100) NOT NULL,
    UNIQUE (distributor_name)
    );

CREATE TABLE IF NOT EXISTS warehouses
(
    id        SERIAL PRIMARY KEY,
    dc_number INT          NOT NULL,
    dc_name   VARCHAR(100) NOT NULL,
    UNIQUE (dc_number, dc_name)
    );

CREATE TABLE IF NOT EXISTS products
(
    id               SERIAL PRIMARY KEY,
    distributor_sku  VARCHAR(50) NOT NULL,
    shell_sku_number INT         NOT NULL,
    description      VARCHAR(255),
    UNIQUE (distributor_sku, shell_sku_number)
    );

CREATE TABLE IF NOT EXISTS units_of_measures
(
    id              SERIAL PRIMARY KEY,
    unit_of_measure VARCHAR(50),
    UNIQUE (unit_of_measure)
    );

CREATE TABLE IF NOT EXISTS sales
(
    id                SERIAL PRIMARY KEY,
    distributor_id    INTEGER REFERENCES distributors (id),
    warehouse_id      INTEGER REFERENCES warehouses (id),
    product_id        INTEGER REFERENCES products (id),
    unit_of_measure_id INTEGER REFERENCES units_of_measures (id),
    sales_date        DATE NOT NULL,
    sales_day         INT  NOT NULL,
    sales_month       INT  NOT NULL,
    sales_year        INT  NOT NULL,
    dfoa_quantity     INTEGER DEFAULT 0,
    non_dfoa_quantity INTEGER DEFAULT 0,
    total_quantity    INTEGER GENERATED ALWAYS AS (dfoa_quantity + non_dfoa_quantity) STORED
    );

CREATE INDEX idx_sales_year_month ON sales (sales_year, sales_month);
CREATE INDEX idx_sales_product ON sales (product_id);
CREATE INDEX idx_sales_warehouse ON sales (warehouse_id);