
CREATE DATABASE IF NOT EXISTS AstroworldDB
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE AstroworldDB;


CREATE TABLE IF NOT EXISTS customers_raw (
    id          INT             NOT NULL,
    name        VARCHAR(255)    NOT NULL,
    dob         DATE     NULL,
    created_at  DATETIME        NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS sales_raw (
    vin             VARCHAR(20)     NOT NULL,
    customer_id     INT             NOT NULL,
    model           VARCHAR(100)    NOT NULL,
    invoice_date    DATE            NOT NULL,
    price           BIGINT          NOT NULL,   -- store in IDR, dots stripped
    created_at      DATETIME        NOT NULL,
    PRIMARY KEY (vin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



CREATE TABLE IF NOT EXISTS after_sales_raw (
    service_ticket  VARCHAR(20)     NOT NULL,
    vin             VARCHAR(20)     NOT NULL,
    customer_id     INT             NOT NULL,
    model           VARCHAR(100)    NOT NULL,
    service_date    DATE            NOT NULL,
    service_type    VARCHAR(10)     NOT NULL,
    created_at      DATETIME        NOT NULL,
    PRIMARY KEY (service_ticket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
