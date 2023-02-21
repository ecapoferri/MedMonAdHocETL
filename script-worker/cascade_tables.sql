-- POSTGRESQL==11+ The following tables are intermediate tables creating integer
-- IDs to be used in creating relationships in production data models. These are
--      created as tables, rather than Views to minimize the processing in
--      production queries (which can add up with so many rows to process in the
--      source table) The snake_case named tables remain as intermediate source
--      tables and the camelCase named tables should be used for production.

-- With so many rows of data (upwards of 4M) and so many companies (over ) These
-- queries should be executed after the main python script (__main__.py) has
--      been executed.

-- Necessary for crosstab. May be redundant for later versions of Postgresql.
CREATE extension IF NOT EXISTS tablefunc;

-- #############################################################################
-- The following 'k_tables'  are preliminary dimension tables for each unique
--      company, media market, and category. Each enerates an arbitrary integer
--      id ('_idx'), acting as a primary key which is used to replace the text
--      values in the production version of the Media Monitors Detail Table.
--      With such a large source dataset, production environments (Excel, PBI)
--      can get bogged down with so much string matching. These keys are joined
--      with other dimension table values (e.g. media type) and/or maintain
--      other values derived from the source table (parent_company)

DROP TABLE IF EXISTS k_category CASCADE;
CREATE TABLE k_category AS
    WITH names AS (SELECT DISTINCT(category) val FROM f_detail)
    SELECT
        val category,
        ROW_NUMBER() OVER ()::INTEGER category_idx
    FROM names
    WHERE val IS NOT NULL
;

DROP TABLE IF EXISTS k_company CASCADE;
CREATE TABLE k_company AS 
    WITH names AS (
        SELECT DISTINCT(company) val, category, parent_company
        FROM f_detail
        -- These three, in combination, should be unique per company. 
        GROUP BY  val, category, parent_company)
    SELECT
        val company,
        parent_company,
        category,
        ROW_NUMBER() OVER ()::INTEGER company_idx
    FROM names
    WHERE val IS NOT NULL
;

-- Likewise for media outlets. This key
DROP TABLE IF EXISTS k_outlet CASCADE;
CREATE TABLE k_outlet AS 
    WITH names AS (SELECT DISTINCT(outlet) val FROM f_detail)
    SELECT
        val outlet,
        ROW_NUMBER() OVER ()::INTEGER outlet_idx
    FROM names
    WHERE val IS NOT NULL
;

DROP TABLE IF EXISTS k_market CASCADE;
CREATE TABLE k_market AS 
    WITH names AS (SELECT DISTINCT(market) val FROM f_detail)
    SELECT
        val market,
        ROW_NUMBER() OVER ()::INTEGER market_idx
    FROM names
    WHERE val IS NOT NULL
;
-- #############################################################################

-- These tables ('fTables' and 'dTables') are production ready tables that can
--      be connected withsimple 'SELECT *' queries. Relationships can be joined
-- on the '_idx' columns to optimize performance in production environments.
DROP TABLE IF EXISTS "fDetail" CASCADE;
CREATE TABLE "fDetail" AS
    SELECT
        c.company_idx,
        o.outlet_idx,
        m.market_idx,

        d.ad_spend,
        d.instance_qty
    FROM f_detail d
        LEFT JOIN k_company c ON c.company = d.company
        LEFT JOIN k_outlet o ON o.outlet = d.outlet
        LEFT JOIN k_market m ON m.market = d.market
;

DROP TABLE IF EXISTS "dOutlet" CASCADE;
CREATE TABLE  "dOutlet" AS
    SELECT
        k.outlet_idx,
        o.name_callsign "Outlet",
        o.type "MediaType"
    FROM d_outlet o LEFT JOIN k_outlet k on o.name_callsign = k.outlet
    WHERE k.outlet_idx IS NOT NULL
;

DROP TABLE IF EXISTS "dCategory" CASCADE;
CREATE TABLE  "dCategory" AS
    SELECT
        k.category_idx,
        c.category_name "Category",
        c.category_group "Group"
    FROM d_category c LEFT JOIN k_category k on k.category = c.category_name
    WHERE k.category_idx IS NOT NULL
;

DROP TABLE IF EXISTS "dMarket" CASCADE;
CREATE TABLE  "dMarket" AS
    SELECT
        market "Market",
        market_idx
    FROM k_market
;

-- Whether a company has purchased media in multiple markets.
DROP TABLE IF EXISTS "dMarketPresence" CASCADE;
CREATE TABLE "dMarketPresence" AS
    SELECT
        DISTINCT(company_idx) company_idx,
        COUNT(DISTINCT(market_idx)) distinct_market_count,
        CASE
            WHEN COUNT(DISTINCT(market_idx)) = 1 THEN TRUE
            WHEN COUNT(DISTINCT(market_idx)) > 1 THEN FALSE
            ELSE NULL
            END
            single_market 
    FROM
        "fDetail"
    GROUP BY company_idx
;

DROP TABLE IF EXISTS "fAdSpendByType" CASCADE;
CREATE TABLE "fAdSpendByType" AS
    SELECT * FROM crosstab(
        -- Unpivoted data
        'SELECT
            d.company_idx,
            o."MediaType",
            SUM(d.ad_spend)
        FROM
            "fDetail" d LEFT JOIN "dOutlet" o USING (outlet_idx)
        GROUP BY d.company_idx, o."MediaType"',
        -- Attribute labels, alpha' order
        'SELECT DISTINCT("MediaType") a FROM "dOutlet" ORDER BY 1'
    ) AS (
        company_idx INTEGER,
        newsp NUMERIC,
        radio NUMERIC,
        tvbc NUMERIC,
        tvca NUMERIC
)
;


-- These are prepared to be joined to 'dCompany' as dimensional values for
--      filtering.
DROP TABLE IF EXISTS "fAdBuysByType" CASCADE;
CREATE TABLE "fAdBuysByType" AS
    SELECT * FROM crosstab(
        'SELECT
            d.company_idx,
            o."MediaType",
            COUNT(*)
        FROM
            "fDetail" d LEFT JOIN "dOutlet" o USING (outlet_idx)
        GROUP BY d.company_idx, o."MediaType"',
        'SELECT DISTINCT("MediaType") FROM "dOutlet" ORDER BY 1'
    ) AS (
        company_idx INTEGER,
        newsp NUMERIC,
        radio NUMERIC,
        tvbc NUMERIC,
        tvca NUMERIC
)
;

DROP TABLE IF EXISTS "dCompany" CASCADE;
CREATE TABLE "dCompany" AS
    SELECT
        c.company_idx::INTEGER company_idx,
        ck.category_idx::INTEGER category_idx,
        c.company "Company",
        c.parent_company "Parent",
        mp.distinct_market_count,
        mp.single_market "SingleMarket",
        t.tvbc::MONEY "TVBC_AdSpend",
        t.tvca::MONEY "TVCA_AdSpend",
        t.radio::MONEY "Radio_AdSpend",
        t.newsp::MONEY "Newsp_AdSpend",
        b.tvbc::INTEGER "TVBC_AdBuys",
        b.tvca::INTEGER "TVCA_AdBuys",
        b.radio::INTEGER "Radio_AdBuys",
        b.newsp::INTEGER "Newsp_AdBuys",
        -- If no count of ad buys for 
        CASE
            WHEN b.tvbc IS NULL THEN TRUE
            ELSE FALSE
            END
            no_tvbc,
        CASE
            WHEN b.tvca IS NOT NULL THEN TRUE
            ELSE FALSE
            END
            has_tvca,

        (
            COALESCE(b.tvbc, 0)
            + COALESCE(b.tvca, 0)
            + COALESCE(b.radio, 0)
            + COALESCE(b.newsp, 0)
        )::INTEGER
            "Total_AdBuys",

        (
            COALESCE(t.tvbc, 0)
            + COALESCE(t.tvca, 0)
            + COALESCE(t.radio, 0)
            + COALESCE(t.newsp, 0)
        )
            "Total_AdSpend",

        ROUND(
            100 * t.tvbc /
                NULLIF(
                    (
                        COALESCE(t.tvbc, 0)
                        + COALESCE(t.tvca, 0)
                        + COALESCE(t.radio, 0)
                        + COALESCE(t.newsp, 0)
                    ),
                    0
                ),
              2
        )::NUMERIC "TVBC_pct"
    FROM k_company c
        LEFT JOIN k_category ck ON c.category = ck.category
        LEFT JOIN "dMarketPresence" mp USING (company_idx)
        LEFT JOIN "fAdSpendByType" t USING (company_idx)
        LEFT JOIN "fAdBuysByType" b USING (company_idx)
;
