CREATE DATABASE output;
USE output;
CREATE TABLE output(
	date_ DATETIME,
    open_price FLOAT,
    highest_price FLOAT,
    lowest_price FLOAT,
    close_price FLOAT,
    volume FLOAT,
    ticker VARCHAR(255)
);

SELECT COUNT(*) FROM output;
TRUNCATE output;