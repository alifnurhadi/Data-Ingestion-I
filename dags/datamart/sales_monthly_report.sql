CREATE TABLE IF NOT EXISTS sales_monthly (
    periode VARCHAR(7) NOT NULL,
    class VARCHAR(10) NOT NULL,
    model VARCHAR(100) NOT NULL,
    total_sales BIGINT NOT NULL,
    last_updated DATETIME NOT NULL, -- Helper column for user filtering
    PRIMARY KEY (periode, class, model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO sales_monthly (periode, class, model, total_sales, last_updated)
SELECT
    DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
    CASE
        WHEN price <= 250000000 THEN 'LOW'
        WHEN price BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN price >= 400000001 THEN 'HIGH'
    END AS class,
    model,
    SUM(price) AS total_sales,
    NOW() AS last_updated
FROM sales_raw
GROUP BY
    periode, class, model
ON DUPLICATE KEY UPDATE
    total_sales = VALUES(total_sales),
    last_updated = VALUES(last_updated);
