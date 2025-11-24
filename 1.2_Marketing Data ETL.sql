– Marketing Campaigns Dataset ETL

-- Create the marketing table
CREATE TABLE marketing_campaigns_table (
    "Ad Account Name"  VARCHAR(150),
    "Campaign name" VARCHAR(150),
    "Delivery status" VARCHAR(150),
    "Delivery level" VARCHAR(150),
    "Reach" INTEGER,
    "Outbound clicks" INTEGER,
    "Landing page views" INTEGER,
    "Result type" VARCHAR(150),
    "Results" INTEGER,
    "Cost per result" NUMERIC,
    "Amount spent (AED)" NUMERIC,
    "CPC (cost per link click)" NUMERIC,
    "Reporting starts" TIMESTAMP
);
---------------------------------------------------------------------------------------------------------
-- Create the cleaning and transformation procedure
CREATE OR REPLACE PROCEDURE clean_marketing_data()
LANGUAGE plpgsql
AS $$
DECLARE
    mode_campaign_name VARCHAR;
    avg_outbound_clicks INTEGER;
    avg_landing_views INTEGER;
    avg_cpc NUMERIC;
BEGIN
    --Get most frequent campaign name
    SELECT "Campaign name"
    INTO mode_campaign_name
    FROM marketing_campaigns
    WHERE "Campaign name" IS NOT NULL
    GROUP BY "Campaign name"
    ORDER BY COUNT(*) DESC
    LIMIT 1;

    --Calculate averages
    SELECT ROUND(AVG("Outbound clicks"))::INTEGER
    INTO avg_outbound_clicks
    FROM marketing_campaigns
    WHERE "Outbound clicks" IS NOT NULL;

    SELECT ROUND(AVG("Landing page views"))::INTEGER
    INTO avg_landing_views
    FROM marketing_campaigns
    WHERE "Landing page views" IS NOT NULL;

    SELECT ROUND(AVG("CPC (cost per link click)"), 2)
    INTO avg_cpc
    FROM marketing_campaigns
    WHERE "CPC (cost per link click)" IS NOT NULL;

    --Clean and insert unique records
    INSERT INTO marketing_campaigns_table (
        "Ad Account Name",
        "Campaign name",
        "Delivery status",
        "Delivery level",
        "Reach",
        "Outbound clicks",
        "Landing page views",
        "Result type",
        "Results",
        "Cost per result",
        "Amount spent (AED)",
        "CPC (cost per link click)",
        "Reporting starts"
    )
    SELECT DISTINCT
        "Ad Account Name",
        INITCAP(COALESCE("Campaign name", mode_campaign_name)),
        INITCAP("Delivery status"),
        INITCAP("Delivery level"),
        "Reach",
        COALESCE("Outbound clicks", avg_outbound_clicks),
        COALESCE("Landing page views", avg_landing_views),
        INITCAP("Result type"),
        "Results",
        "Cost per result",
        "Amount spent (AED)",
        COALESCE("CPC (cost per link click)", avg_cpc),
        "Reporting starts"
    FROM marketing_campaigns;
END;
$$;

-- Run the procedure
CALL clean_marketing_data();
----------------------------------------------------------------------------------------------------------
-- Check the staging_marketing_campaigns
SELECT * FROM marketing_campaigns_table;
SELECT COUNT(*) FROM marketing_campaigns_table;
----------------------------------------------------------------------------------------------------------
-- Missing values in original and staged tables
SELECT
    COUNT(*) FILTER (WHERE "Campaign name" IS NULL) AS campaign_name_nulls,
    COUNT(*) FILTER (WHERE "Outbound clicks" IS NULL) AS outbound_clicks_nulls,
    COUNT(*) FILTER (WHERE "Landing page views" IS NULL) AS landing_page_views_nulls,
    COUNT(*) FILTER (WHERE "CPC (cost per link click)" IS NULL) AS cpc_nulls
FROM marketing_campaigns;

SELECT
    COUNT(*) FILTER (WHERE "Campaign name" IS NULL) AS campaign_name_nulls,
    COUNT(*) FILTER (WHERE "Outbound clicks" IS NULL) AS outbound_clicks_nulls,
    COUNT(*) FILTER (WHERE "Landing page views" IS NULL) AS landing_page_views_nulls,
    COUNT(*) FILTER (WHERE "CPC (cost per link click)" IS NULL) AS cpc_nulls
FROM marketing_campaigns_table;
----------------------------------------------------------------------------------------------------------
--Check for duplicate rows in table
SELECT 
    "Ad Account Name",
    "Campaign name",
    "Delivery status",
    "Delivery level",
    "Reach",
    "Outbound clicks",
    "Landing page views",
    "Result type",
    "Results",
    "Cost per result",
    "Amount spent (AED)",
    "CPC (cost per link click)",
    "Reporting starts",
    COUNT(*) AS duplicate_count
FROM marketing_campaigns_table
GROUP BY 
    "Ad Account Name",
    "Campaign name",
    "Delivery status",
    "Delivery level",
    "Reach",
    "Outbound clicks",
    "Landing page views",
    "Result type",
    "Results",
    "Cost per result",
    "Amount spent (AED)",
    "CPC (cost per link click)",
    "Reporting starts"
HAVING COUNT(*) > 1;