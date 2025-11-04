{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='TerritoryKey',
        settings={'allow_nullable_key': 1}
    )
}}

WITH territories AS (
    SELECT
        SalesTerritoryKey AS TerritoryKey,
        Region,
        Country,
        Continent
    FROM {{ source('bronze', 'Territories') }}
)

SELECT
    -- Primary Key
    TerritoryKey,
    
    -- Geography Hierarchy
    COALESCE(Continent, 'Unknown') AS Continent,
    COALESCE(Country, 'Unknown') AS Country,
    COALESCE(Region, 'Unknown') AS Region,
    
    -- Full Path (for hierarchy)
    concat(
        COALESCE(Continent, 'Unknown'), ' > ',
        COALESCE(Country, 'Unknown'), ' > ',
        COALESCE(Region, 'Unknown')
    ) AS GeographyPath,
    
    -- Metadata
    now() AS LoadDate
    
FROM territories;