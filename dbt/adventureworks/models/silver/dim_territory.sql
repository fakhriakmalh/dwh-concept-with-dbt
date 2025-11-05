{{
    config(
        materialized='table',
        schema='silver',
        engine='MergeTree()',
        order_by='TerritoryKey',
        settings={'allow_nullable_key': 1}
    )
}}

-- CTE untuk pembersihan nama kolom dan penanganan NULL sederhana
WITH cleaned_territories AS (
    SELECT
        -- Mengubah nama kunci sumber menjadi kunci dimensi
        SalesTerritoryKey AS TerritoryKey,
        
        -- Memastikan kolom deskriptif tidak NULL
        COALESCE(Region, '') AS Region,
        COALESCE(Country, '') AS Country,
        COALESCE(Continent, '') AS Continent
        
    FROM {{ source('bronze', 'Territories') }}
)

-- Final SELECT: Derivasi Atribut dan Penambahan Metadata
SELECT
    -- Primary Key (Surrogate Key)
    TerritoryKey,
    
    -- Geography Hierarchy (Menggunakan 'Unknown' hanya pada tampilan akhir)
    CASE WHEN Continent = '' THEN 'Unknown' ELSE Continent END AS Continent,
    CASE WHEN Country = '' THEN 'Unknown' ELSE Country END AS Country,
    CASE WHEN Region = '' THEN 'Unknown' ELSE Region END AS Region,
    
    -- Full Path (untuk analisis hierarkis cepat)
    concat(
        CASE WHEN Continent = '' THEN 'Unknown' ELSE Continent END, ' > ',
        CASE WHEN Country = '' THEN 'Unknown' ELSE Country END, ' > ',
        CASE WHEN Region = '' THEN 'Unknown' ELSE Region END
    ) AS GeographyPath,
    
    -- Metadata
    now() AS LoadDate
    
FROM cleaned_territories