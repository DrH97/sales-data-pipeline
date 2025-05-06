-- 1. Total quantity (DFOA + Non-DFOA) per product (Sales revenue would need prices as well)
SELECT
    p.shell_sku_number,
    p.distributor_sku,
    p.description,
    SUM(s.total_quantity) AS total_quantity
FROM
    sales s
        JOIN
    products p ON s.product_id = p.product_id
GROUP BY
    p.shell_sku_number, p.distributor_sku, p.description
ORDER BY
    total_quantity DESC;

-- 2. Quantity breakdown by warehouse
SELECT
    w.dc_name,
    w.dc_number,
    SUM(s.dfoa_quantity) AS total_dfoa,
    SUM(s.non_dfoa_quantity) AS total_non_dfoa,
    SUM(s.total_quantity) AS total_quantity
FROM
    sales s
        JOIN
    warehouses w ON s.warehouse_id = w.warehouse_id
GROUP BY
    w.dc_name, w.dc_number
ORDER BY
    total_quantity DESC;

-- 3. Top 5 products by total quantity (By revenue would need prices as well)
SELECT
    p.shell_sku_number,
    p.description,
    SUM(s.total_quantity) AS total_quantity
FROM
    sales s
        JOIN
    products p ON s.product_id = p.product_id
GROUP BY
    p.shell_sku_number, p.description
ORDER BY
    total_quantity DESC
    LIMIT 5;

-- 4. Monthly sales report with quantity breakdown (With revenue would need prices as well)
SELECT
    sales_month,
    SUM(dfoa_quantity) AS monthly_dfoa,
    SUM(non_dfoa_quantity) AS monthly_non_dfoa,
    SUM(total_quantity) AS monthly_total
FROM
    sales
GROUP BY
    sales_month
ORDER BY
    month;