{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='CustomerKey',
        settings={'allow_nullable_key': 1}
    )
}}

WITH customers AS (
    SELECT
        CustomerKey,
        
        -- Name
        COALESCE(Prefix, '') AS Prefix,
        FirstName,
        COALESCE(MiddleName, '') AS MiddleName,
        LastName,
        COALESCE(Suffix, '') AS Suffix,
        
        -- Contact
        COALESCE(EmailAddress, '') AS EmailAddress,
        COALESCE(Phone, '') AS Phone,
        
        -- Address
        COALESCE(AddressLine1, '') AS AddressLine1,
        COALESCE(AddressLine2, '') AS AddressLine2,
        COALESCE(City, '') AS City,
        COALESCE(StateProvince, '') AS StateProvince,
        COALESCE(CountryRegion, '') AS CountryRegion,
        COALESCE(PostalCode, '') AS PostalCode,
        
        -- Demographics
        COALESCE(BirthDate, toDate('1900-01-01')) AS BirthDate,
        COALESCE(MaritalStatus, 'U') AS MaritalStatus,
        COALESCE(Gender, 'U') AS Gender,
        COALESCE(YearlyIncome, 0) AS YearlyIncome,
        COALESCE(TotalChildren, 0) AS TotalChildren,
        COALESCE(NumberChildrenAtHome, 0) AS NumberChildrenAtHome,
        COALESCE(Education, 'Unknown') AS Education,
        COALESCE(Occupation, 'Unknown') AS Occupation,
        COALESCE(HomeOwner, 'Unknown') AS HomeOwner
        
    FROM {{ source('bronze', 'Customers') }}
)

SELECT
    -- Primary Key
    CustomerKey,
    
    -- Full Name
    concat(
        CASE WHEN Prefix != '' THEN concat(Prefix, ' ') ELSE '' END,
        FirstName, ' ',
        CASE WHEN MiddleName != '' THEN concat(MiddleName, ' ') ELSE '' END,
        LastName,
        CASE WHEN Suffix != '' THEN concat(' ', Suffix) ELSE '' END
    ) AS FullName,
    
    FirstName,
    LastName,
    
    -- Contact
    EmailAddress,
    Phone,
    
    -- Address
    AddressLine1,
    City,
    StateProvince,
    CountryRegion,
    PostalCode,
    
    -- Demographics
    BirthDate,
    CASE 
        WHEN BirthDate != toDate('1900-01-01') 
        THEN dateDiff('year', BirthDate, today())
        ELSE NULL
    END AS Age,
    
    MaritalStatus,
    CASE MaritalStatus
        WHEN 'M' THEN 'Married'
        WHEN 'S' THEN 'Single'
        ELSE 'Unknown'
    END AS MaritalStatusDesc,
    
    Gender,
    CASE Gender
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'Unknown'
    END AS GenderDesc,
    
    YearlyIncome,
    CASE
        WHEN YearlyIncome < 25000 THEN 'Low'
        WHEN YearlyIncome < 75000 THEN 'Medium'
        WHEN YearlyIncome < 150000 THEN 'High'
        ELSE 'Very High'
    END AS IncomeGroup,
    
    TotalChildren,
    NumberChildrenAtHome,
    Education,
    Occupation,
    HomeOwner,
    
    -- Metadata
    now() AS LoadDate
    
FROM customers;