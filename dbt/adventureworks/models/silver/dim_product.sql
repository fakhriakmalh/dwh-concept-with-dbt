{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='ProductKey',
        settings={'allow_nullable_key': 1}
    )
}}

WITH products AS (
    SELECT
        ProductKey,
        ProductName,
        ProductSubcategoryKey,
        ProductColor,
        ProductSize,
        ProductPrice,
        ProductCost
    FROM {{ source('bronze', 'Products') }}
),

subcategories AS (
    SELECT
        ProductSubcategoryKey,
        SubcategoryName,
        ProductCategoryKey
    FROM {{ source('bronze', 'Product_Subcategories') }}
),

categories AS (
    SELECT
        ProductCategoryKey,
        CategoryName
    FROM {{ source('bronze', 'Product_Categories') }}
)

SELECT
    -- Primary Key
    p.ProductKey,
    
    -- Product Info
    p.ProductName,
    COALESCE(p.ProductColor, 'N/A') AS Color,
    COALESCE(p.ProductSize, 'N/A') AS Size,
    p.ProductPrice,
    p.ProductCost,
    
    -- Subcategory
    COALESCE(sc.ProductSubcategoryKey, -1) AS ProductSubcategoryKey,
    COALESCE(sc.SubcategoryName, 'Unknown') AS ProductSubcategoryName,
    
    -- Category
    COALESCE(c.ProductCategoryKey, -1) AS ProductCategoryKey,
    COALESCE(c.CategoryName, 'Unknown') AS ProductCategoryName,
    
    -- Metadata
    now() AS LoadDate
    
FROM products p
LEFT JOIN subcategories sc 
    ON p.ProductSubcategoryKey = sc.ProductSubcategoryKey
LEFT JOIN categories c 
    ON sc.ProductCategoryKey = c.ProductCategoryKey
