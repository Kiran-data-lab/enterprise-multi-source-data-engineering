IF NOT EXISTS (
    SELECT 1 FROM SQLSERVER_SRC.INVENTORY
)
BEGIN
    INSERT INTO SQLSERVER_SRC.INVENTORY
    (
        PRODUCT_ID,
        WAREHOUSE_ID,
        QUANTITY_ON_HAND,
        REORDER_LEVEL,
        LAST_STOCK_UPDATE
    )
    SELECT
        P.PRODUCT_ID,
        W.WAREHOUSE_ID,
        (P.PRODUCT_ID * 13 + W.WAREHOUSE_NO * 29) % 500,
        20 + (P.PRODUCT_ID % 80),
        DATEADD(
            DAY,
            -((P.PRODUCT_ID + W.WAREHOUSE_NO) % 60),
            SYSDATETIME()
        )
    FROM SQLSERVER_SRC.PRODUCTS P
    CROSS JOIN
    (
        VALUES
            (1, 'WH_HYD'),
            (2, 'WH_BLR'),
            (3, 'WH_MUM')
    ) W(WAREHOUSE_NO, WAREHOUSE_ID);
END;
GO