SELECT 
    P.PRODUCT_NAME,
    SUM(F.QUANTITY) AS TOTAL_QUANTITY,
    AVG(F.PRICE) AS AVG_PRICE
FROM 
    FACT_ORDERS F
JOIN 
    DIM_PRODUCT P ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY 
    P.PRODUCT_NAME
ORDER BY 
    TOTAL_QUANTITY DESC;