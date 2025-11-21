CREATE DATABASE IF NOT EXISTS DE_2025_data_warehouse;

USE DE_2025_data_warehouse;

CREATE TABLE IF NOT EXISTS campaign (
    id INT PRIMARY KEY,
    job_id TEXT NULL,
    dates DATE NULL,
    hours INT NULL,
    click INT NULL,
    conversion INT NULL,
    qualified INT NULL,
    unqualified INT NULL,
    group_id TEXT NULL,
    campaign_id TEXT NULL,
    publisher_id TEXT NULL,
    bid_set DOUBLE NULL,
    spend_hour DOUBLE NULL,
    sources TEXT NULL
);

SELECT * FROM DE_2025_data_warehouse.campaign;
