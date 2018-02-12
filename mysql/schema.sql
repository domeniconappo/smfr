CREATE DATABASE IF NOT EXISTS smfr;
CREATE DATABASE IF NOT EXISTS smfr_test;
GRANT ALL PRIVILEGES ON smfr.* TO 'root'@'%' IDENTIFIED BY 'example';
GRANT ALL PRIVILEGES ON smfr.* TO 'root'@'localhost' IDENTIFIED BY 'example';
GRANT ALL PRIVILEGES ON smfr_test.* TO 'root'@'%' IDENTIFIED BY 'example';
GRANT ALL PRIVILEGES ON smfr_test.* TO 'root'@'localhost' IDENTIFIED BY 'example';
