IF NOT EXISTS (
    SELECT 1 FROM SQLSERVER_SRC.PRODUCTS
)
BEGIN
    ;WITH Numbers AS
    (
        SELECT 1 AS N
        UNION ALL
        SELECT N + 1
        FROM Numbers
        WHERE N < 500
    )
    INSERT INTO SQLSERVER_SRC.PRODUCTS
    (
        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY,
        BRAND,
        UNIT_PRICE,
        PRODUCT_STATUS,
        LAUNCH_DATE
    )
    SELECT
        N,
        CONCAT('Product-', N),
        CASE N % 5
            WHEN 0 THEN 'Electronics'
            WHEN 1 THEN 'Furniture'
            WHEN 2 THEN 'Clothing'
            WHEN 3 THEN 'Grocery'
            ELSE 'Accessories'
        END,
        CASE N % 4
            WHEN 0 THEN 'Samsung'
            WHEN 1 THEN 'Dell'
            WHEN 2 THEN 'HP'
            ELSE 'Generic'
        END,
        CAST(50 + ((N * 1735) % 495000) / 100.0 AS DECIMAL(12,2)),
        CASE WHEN N % 20 = 0 THEN 'DISCONTINUED'
             ELSE 'ACTIVE'
        END,
        DATEADD(DAY, N % 1200, '2023-01-01')
    FROM Numbers
    OPTION (MAXRECURSION 0);
END;
GO