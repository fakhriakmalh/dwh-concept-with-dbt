{{
    config(
        materialized='table',
        schema='silver',
        engine='MergeTree()',
        order_by='CustomerKey',
        settings={'allow_nullable_key': 1}
    )
}}

-- CTE untuk membersihkan dan menyiapkan kolom
WITH cleaned_customers AS (
    SELECT
        CustomerKey,
        
        -- Name & Contact
        COALESCE(Prefix, '') AS Prefix,
        FirstName,
        LastName,
        COALESCE(EmailAddress, '') AS EmailAddress,
        
        -- Demografi (Konversi dan Pembersihan Tipe Data)
        
        -- 1. Perbaikan BirthDate (dari error sebelumnya: String vs Date)
        COALESCE(
            toDateOrNull(BirthDate), 
            toDate('1900-01-01')     
        ) AS BirthDate,
        
        COALESCE(MaritalStatus, 'U') AS MaritalStatus,
        COALESCE(Gender, 'U') AS Gender,
        
        -- -- 2. PERBAIKAN ERROR BARU: Mengubah nilai pengganti 0 menjadi '0' (String)
        AnnualIncome,
        TotalChildren,
        -- COALESCE(AnnualIncome, '0') AS YearlyIncome_String, 
        -- COALESCE(TotalChildren, '0') AS TotalChildren_String,
        
        COALESCE(EducationLevel, 'Unknown') AS Education, 
        COALESCE(Occupation, 'Unknown') AS Occupation,
        COALESCE(HomeOwner, 'Unknown') AS HomeOwner
        
    FROM {{ source('bronze', 'Customers') }}
)

-- Final SELECT: Konversi String Numerik menjadi Tipe Numerik (Float64/Int32)
SELECT
    -- Primary Key
    CustomerKey,
    
    -- Full Name
    concat(
        CASE WHEN Prefix != '' THEN concat(Prefix, ' ') ELSE '' END,
        FirstName, ' ',
        LastName
    ) AS FullName,
    
    FirstName,
    LastName,
    EmailAddress,
    
    -- Demographics
    BirthDate,
    
    -- Age Calculation
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
    
    -- KONVERSI: Mengonversi String yang sudah dibersihkan menjadi Float64/Int32
    -- toFloat64(YearlyIncome_String) AS YearlyIncome,
    -- toInt32(TotalChildren_String) AS TotalChildren,
    
    -- -- Derivasi: Income Group (Menggunakan kolom yang sudah dikonversi)
    -- CASE
    --     WHEN YearlyIncome < 25000 THEN 'Low'
    --     WHEN YearlyIncome < 75000 THEN 'Medium'
    --     WHEN YearlyIncome < 150000 THEN 'High'
    --     ELSE 'Very High'
    -- END AS IncomeGroup,
    
    Education,
    Occupation,
    HomeOwner,
    
    -- Metadata
    now() AS LoadDate
    
FROM cleaned_customers