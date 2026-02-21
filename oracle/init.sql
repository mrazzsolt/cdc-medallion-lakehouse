-- =============================================================
-- oracle/init.sql
-- This script runs automatically when the Oracle container
-- starts for the first time. It is executed as SYS (SYSDBA)
-- inside XEPDB1
-- =============================================================


-- ---------------------------------------------------------------
-- PART 1: CDB ROOT context
-- Oracle has two levels: CDB (the outer container database) and
-- PDB (the inner pluggable database where our app lives).
-- Some settings must be configured at the CDB level.
-- ---------------------------------------------------------------

-- Switch from XEPDB1 (PDB) to CDB$ROOT (the outer container)

ALTER SESSION SET CONTAINER=CDB$ROOT;

-- SUPPLEMENTAL LOGGING (database level)
-- Oracle normally only logs the minimum needed to undo a change.
-- Supplemental logging makes Oracle write the FULL row data into
-- the redo log — Debezium needs this to know WHAT changed.

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- DEBEZIUM USER (LogMiner)
-- Debezium reads Oracle's redo logs via a feature called LogMiner.
-- It needs its own dedicated user with special read permissions.
-- Common users in Oracle CDB must start with "C##".
-- "CONTAINER=ALL" means the user/grant applies across all PDBs.

CREATE USER c##debezium IDENTIFIED BY dbzpassword CONTAINER=ALL;

-- Grant the necessary permissions to the Debezium user for LogMiner.
GRANT CREATE SESSION TO c##debezium CONTAINER=ALL;
-- Grant permission to switch into a PDB
GRANT SET CONTAINER TO c##debezium CONTAINER=ALL;

-- LogMiner privileges: read on redo logs
GRANT LOGMINING TO c##debezium CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##debezium CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##debezium CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##debezium CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##debezium CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##debezium CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##debezium CONTAINER=ALL;
GRANT CREATE TABLE TO c##debezium CONTAINER=ALL;

-- Permissions for reading Oracle internal metadata views
GRANT SELECT ON v_$database TO c##debezium CONTAINER=ALL;
GRANT SELECT ON v_$log TO c##debezium CONTAINER=ALL;
GRANT SELECT ON v_$logfile TO c##debezium CONTAINER=ALL;
GRANT SELECT ON v_$logmnr_logs TO c##debezium CONTAINER=ALL;
GRANT SELECT ON V_$logmnr_contents TO c##debezium CONTAINER=ALL;
GRANT SELECT ON v_$logmnr_parameters TO c##debezium CONTAINER=ALL;
GRANT SELECT ON v_$archived_log TO c##debezium CONTAINER=ALL;
GRANT SELECT ON v_$archive_dest_status TO c##debezium CONTAINER=ALL;

-- ---------------------------------------------------------------
-- PART 2: Back to XEPDB1 (our application PDB)
-- Everything from here on lives inside the pluggable database
-- where our actual application data will be stored.
-- ---------------------------------------------------------------

-- Back to the PDB
ALTER SESSION SET CONTAINER=XEPDB1;

-- APP USER GRANTS
--User was created by the Docker image

GRANT CREATE SESSION TO app;
GRANT CREATE TABLE TO app;
GRANT CREATE SEQUENCE TO app;
GRANT CREATE TRIGGER TO app;
GRANT UNLIMITED TABLESPACE TO app;

-- ---------------------------------------------------------------
-- SEQUENCES
-- Oracle does not have AUTO_INCREMENT like MySQL.
-- Sequences are separate objects that generate unique numbers.
-- We use them to produce primary key values.
-- ---------------------------------------------------------------

-- customers.customer_id sequence
CREATE SEQUENCE app.customer_seq
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- orders.order_id sequence
CREATE SEQUENCE app.order_seq
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

CREATE SEQUENCE app.product_seq   
    START WITH 1 
    INCREMENT BY 1 
    NOCACHE 
    NOCYCLE;

CREATE SEQUENCE app.category_seq  
    START WITH 1 
    INCREMENT BY 1 
    NOCACHE 
    NOCYCLE;


-- ---------------------------------------------------------------
-- TABLES
-- Three tables that model a simple e-commerce scenario:
--   CUSTOMERS --> the people placing orders
--   ORDERS    --> the orders they place
--   ORDER_ITEMS --> the individual products inside an order
-- ---------------------------------------------------------------

-- =====================================================
-- TABLE: app.product_categories
-- Lookup table for product categories (e.g. Electronics, Food).
-- No foreign keys — this is the "root" of the product hierarchy.
-- =====================================================
CREATE TABLE app.product_categories (
    category_id         NUMBER(5)       PRIMARY KEY,
    category_name       VARCHAR2(100)   NOT NULL,
    created_at          TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    created_by          VARCHAR2(50)    DEFAULT 'SYSTEM' NOT NULL,
    modified_at         TIMESTAMP,
    modified_by         VARCHAR2(50)
);

-- =====================================================
-- TABLE: app.products
-- A product can belong to multiple categories (resolved
-- via the products_x_category junction table below).
-- =====================================================
CREATE TABLE app.products (
    item_id             NUMBER          PRIMARY KEY,
    product_name        VARCHAR2(200)   NOT NULL,
    quantity            NUMBER(5)       NOT NULL,
    unit_price          NUMBER(10, 2)   NOT NULL,
    created_at          TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    created_by          VARCHAR2(50)    DEFAULT 'SYSTEM' NOT NULL,
    modified_at         TIMESTAMP,
    modified_by         VARCHAR2(50)
);

-- =====================================================
-- TABLE: app.products_x_category
-- Junction table: resolves many-to-many between products
-- and categories. One product can be in many categories,
-- one category can contain many products.
-- Composite PK prevents duplicate assignments.
-- =====================================================
CREATE TABLE app.products_x_category (
    item_id             NUMBER(5)       NOT NULL,
    category_id         NUMBER(5)       NOT NULL,
    -- Composite primary key: each product-category pair is unique
    CONSTRAINT pk_products_x_category
        PRIMARY KEY (item_id, category_id),
    CONSTRAINT fk_pxc_product
        FOREIGN KEY (item_id)       REFERENCES app.products(item_id),
    CONSTRAINT fk_pxc_category
        FOREIGN KEY (category_id)   REFERENCES app.product_categories(category_id)
);

-- =====================================================
-- TABLE: app.customers
-- =====================================================
CREATE TABLE app.customers (
    customer_id         NUMBER          PRIMARY KEY,
    first_name          VARCHAR2(50)    NOT NULL,
    last_name           VARCHAR2(50)    NOT NULL,
    email               VARCHAR2(100)   NOT NULL UNIQUE,
    mobile              VARCHAR2(20),
    city                VARCHAR2(50),
    country             VARCHAR2(50),
    created_at          TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    created_by          VARCHAR2(50)    DEFAULT 'SYSTEM' NOT NULL,
    modified_at         TIMESTAMP,
    modified_by         VARCHAR2(50)
);

-- =====================================================
-- TABLE: app.orders
-- An order ties a customer to a product they purchased.
-- Shipping address is stored here (city/country at time of order).
-- =====================================================
CREATE TABLE app.orders (
    order_id            NUMBER          PRIMARY KEY,
    customer_id         NUMBER          NOT NULL,
    product_item_id     NUMBER          NOT NULL,
    ordered_quantity    NUMBER(5)       NOT NULL,
    order_status        VARCHAR2(20)    DEFAULT 'PENDING' NOT NULL,
    city                VARCHAR2(50),
    country             VARCHAR2(50),
    street              VARCHAR2(100),
    additional_info     VARCHAR2(500),
    created_at          TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    created_by          VARCHAR2(50)    DEFAULT 'SYSTEM' NOT NULL,
    modified_at         TIMESTAMP,
    modified_by         VARCHAR2(50),
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)       REFERENCES app.customers(customer_id),
    -- Orders reference a product, not the other way around
    CONSTRAINT fk_orders_product
        FOREIGN KEY (product_item_id)   REFERENCES app.products(item_id),
    CONSTRAINT chk_orders_status
        CHECK (order_status IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'))
);


-- ---------------------------------------------------------------
-- TRIGGER: auto-update updated_at on ORDERS
-- A trigger is a piece of code that Oracle runs automatically
-- before or after a DML operation (INSERT, UPDATE, DELETE).
-- This one keeps updated_at in sync whenever a row is updated.
-- ---------------------------------------------------------------

-- Trigger for app.orders
CREATE OR REPLACE TRIGGER app.trg_orders_modified_at
    BEFORE UPDATE ON app.orders FOR EACH ROW
BEGIN
    :NEW.modified_at := SYSTIMESTAMP;
    :NEW.modified_by := USER;
END;
/

CREATE OR REPLACE TRIGGER app.trg_orders_created_at
    BEFORE INSERT ON app.orders FOR EACH ROW
BEGIN
    :NEW.created_at := SYSTIMESTAMP;
    :NEW.created_by := USER;
END;
/

-- Trigger for app.customers
CREATE OR REPLACE TRIGGER app.trg_customers_modified_at
    BEFORE UPDATE ON app.customers FOR EACH ROW
BEGIN
    :NEW.modified_at := SYSTIMESTAMP;
    :NEW.modified_by := USER;
END;
/
CREATE OR REPLACE TRIGGER app.trg_customers_created_at
    BEFORE INSERT ON app.customers FOR EACH ROW
BEGIN
    :NEW.created_at := SYSTIMESTAMP;
    :NEW.created_by := USER;
END;
/

-- Trigger for app.products
CREATE OR REPLACE TRIGGER app.trg_products_modified_at
    BEFORE UPDATE ON app.products FOR EACH ROW
BEGIN
    :NEW.modified_at := SYSTIMESTAMP;
    :NEW.modified_by := USER;
END;
/

CREATE OR REPLACE TRIGGER app.trg_products_created_at
    BEFORE INSERT ON app.products FOR EACH ROW
BEGIN
    :NEW.created_at := SYSTIMESTAMP;
    :NEW.created_by := USER;
END;
/


-- ---------------------------------------------------------------
-- SUPPLEMENTAL LOGGING (table level)
-- Database-level supplemental logging (done in Part 1) only logs
-- the primary key columns by default.
-- "ALL COLUMNS" means log EVERY column before and after a change —
-- Debezium needs this to produce complete before/after payloads.
-- ---------------------------------------------------------------

ALTER TABLE app.customers           ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE app.orders              ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE app.products            ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE app.product_categories  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE app.products_x_category ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- ---------------------------------------------------------------
-- VERIFICATION
-- These SELECTs run at the end so you can see the results in
-- the Docker logs and confirm everything was created correctly.
-- ---------------------------------------------------------------

SELECT 'Tables created: ' || COUNT(*) AS result
FROM all_tables
WHERE owner = 'APP';

SELECT 'Supplemental logging enabled for: ' || COUNT(*) AS result
FROM all_log_groups
WHERE owner = 'APP';