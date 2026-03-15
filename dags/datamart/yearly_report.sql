CREATE TABLE IF NOT EXISTS customer_service_yearly (
    periode VARCHAR(4) NOT NULL,
    vin VARCHAR(20) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    address TEXT,
    count_service INT NOT NULL,
    priority VARCHAR(10) NOT NULL,
    last_updated DATETIME NOT NULL, -- Helper column for user filtering
    PRIMARY KEY (periode, vin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


INSERT INTO customer_service_yearly (periode, vin, customer_name, address, count_service, priority, last_updated)
WITH LatestAddress AS (
    SELECT a.customer_id, a.address
    FROM customer_address a
    INNER JOIN (
        SELECT customer_id, MAX(id) AS latest_id
        FROM customer_address a
        GROUP BY customer_id
    ) max_addr ON a.id = max_addr.latest_id
),
ServiceAgg AS (
    SELECT
        DATE_FORMAT(service_date, '%Y') AS periode,
        vin,
        customer_id,
        COUNT(service_ticket) AS count_service
    FROM after_sales_raw
    GROUP BY DATE_FORMAT(service_date, '%Y'), vin, customer_id
)
SELECT
    s.periode,
    s.vin,
    c.name AS customer_name,
    la.address,
    s.count_service,
    CASE
        WHEN s.count_service < 5 THEN 'LOW'
        WHEN s.count_service BETWEEN 5 AND 10 THEN 'MED'
        WHEN s.count_service > 10 THEN 'HIGH'
    END AS priority,
    NOW() AS last_updated
FROM ServiceAgg s
JOIN customers_raw c ON s.customer_id = c.id
LEFT JOIN LatestAddress la ON s.customer_id = la.customer_id
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    address = VALUES(address),
    count_service = VALUES(count_service),
    priority = VALUES(priority),
    last_updated = VALUES(last_updated);
