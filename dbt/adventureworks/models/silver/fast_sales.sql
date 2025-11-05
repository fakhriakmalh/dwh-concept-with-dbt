{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(DateKey, ProductKey, CustomerKey)',
        settings={'allow_nullable_key': 1}
    )
}}

WITH sales_2015 AS (
    SELECT * FROM {{ source('bronze', 'Sales_2015') }}
),

sales_2016 AS (
    SELECT * FROM {{ source('bronze', 'Sales_2016') }}
),

sales_2017 AS (
    SELECT * FROM {{ source('bronze', 'Sales_2017') }}
),

all_sales AS (
    SELECT * FROM sales_2015
    UNION ALL
    SELECT * FROM sales_2016
    UNION ALL
    SELECT * FROM sales_2017
)

SELECT
    -- Surrogate Key
    toUInt64(cityHash64(concat(
        toString(OrderDate),
        toString(ProductKey),
        toString(CustomerKey),
        toString(TerritoryKey),
        toString(OrderNumber)
    ))) AS SalesKey,
    
    -- Foreign Keys (Dimensions)
    toInt32(formatDateTime(toDate(OrderDate), '%Y%m%d')) AS DateKey,
    ProductKey,
    CustomerKey,
    TerritoryKey,
    
    -- Degenerate Dimension
    OrderNumber,
    OrderLineItem,
    
    -- Measures (Metrics)
    COALESCE(OrderQuantity, 0) AS OrderQuantity,
    COALESCE(UnitPrice, 0) AS UnitPrice,
    COALESCE(ProductStandardCost, 0) AS ProductStandardCost,
    
    -- Calculated Measures
    COALESCE(OrderQuantity, 0) * COALESCE(UnitPrice, 0) AS SalesAmount,
    COALESCE(OrderQuantity, 0) * COALESCE(ProductStandardCost, 0) AS TotalCost,
    (COALESCE(OrderQuantity, 0) * COALESCE(UnitPrice, 0)) - 
    (COALESCE(OrderQuantity, 0) * COALESCE(ProductStandardCost, 0)) AS ProfitAmount,
    
    -- Discount/Tax
    COALESCE(DiscountAmount, 0) AS DiscountAmount,
    COALESCE(TaxAmount, 0) AS TaxAmount,
    COALESCE(Freight, 0) AS Freight,
    
    -- Net Sales
    (COALESCE(OrderQuantity, 0) * COALESCE(UnitPrice, 0)) - 
    COALESCE(DiscountAmount, 0) AS NetSalesAmount,
    
    -- Profit Margin
    CASE 
        WHEN COALESCE(OrderQuantity, 0) * COALESCE(UnitPrice, 0) > 0 
        THEN ((COALESCE(OrderQuantity, 0) * COALESCE(UnitPrice, 0)) - 
              (COALESCE(OrderQuantity, 0) * COALESCE(ProductStandardCost, 0))) / 
             (COALESCE(OrderQuantity, 0) * COALESCE(UnitPrice, 0)) * 100
        ELSE 0
    END AS ProfitMarginPercent,
    
    -- Metadata
    OrderDate,
    now() AS LoadDate
    
FROM all_sales
WHERE 
    OrderDate IS NOT NULL
    AND ProductKey IS NOT NULL
    AND CustomerKey IS NOT NULL