{{config(materialized = 'table', schema='SILVER')}}

WITH source AS (
    SELECT data
    FROM {{source('bronze','polymarket_markets_raw')}}
)

SELECT 
    data:id::NUMBER(38,0)                AS id,
    data:question::VARCHAR               AS question,
    data:conditionId::VARCHAR            AS condition_id,
    data:slug::VARCHAR                   AS slug,
    data:endDate::TIMESTAMP_NTZ          AS end_date,
    data:liquidity::FLOAT                AS liquidity,
    data:startDate::TIMESTAMP_NTZ        AS start_date,
    data:description::VARCHAR            AS description,
    data:outcomes::ARRAY                 AS outcomes,
    data:outcomePrices::ARRAY            AS outcome_prices,
    data:volume::FLOAT                   AS volume,
    data:active::BOOLEAN                 AS active,
    data:closed::BOOLEAN                 AS closed,
    data:marketMakerAddress::VARCHAR     AS market_maker_address,
    data:createdAt::TIMESTAMP_NTZ        AS created_at,
    data:featured::BOOLEAN               AS featured,
    data:groupItemTitle::VARCHAR         AS group_item_title,
    data:questionId::VARCHAR             AS question_id,
    data:volumeNum::FLOAT                AS volume_num,
    data:liquidityNum::FLOAT             AS liquidity_num,
    data:volume24hr::FLOAT               AS volume_24hr,
    data:volume1wk::FLOAT                AS volume_1wk,
    data:volume1mo::FLOAT                AS volume_1mo,
    data:volume1yr::FLOAT                AS volume_1yr,
    data:clobTokenIds::ARRAY             AS clob_token_ids,
    data:acceptingOrders::BOOLEAN        AS accepting_orders
FROM source
WHERE data:id IS NOT NULL