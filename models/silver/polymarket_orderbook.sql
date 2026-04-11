{{ config(materialized='table')}}

WITH source AS (
    SELECT 
        data
    FROM 
        {{ source('bronze', 'polymarket_orderbook_stg')}}
)

SELECT 
    data:id::varchar AS id,
    data:maker::varchar AS maker,
    data:taker::varchar AS taker,
    data:makerAssetId::varchar AS maker_asset_id,
    data:takerAssetId::varchar AS taker_asset_id,
    data:makerAmountFilled::float AS maker_amount_filled,
    data:takerAmountFilled::float AS taker_amount_filled,
    data:fee::float AS fee,
    data:timestamp::timestamp_ntz AS timestamp,
    data:transactionHash::varchar AS transaction_hash
FROM
    source 
WHERE 
    data:id IS NOT NULL