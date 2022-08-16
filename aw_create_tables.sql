DROP DATABASE IF EXISTS chicago_bi;
CREATE DATABASE chicago_bi;

DROP TABLE IF EXISTS MSDSCourseCatalog;

\c chicago_bi;

/*CREATE TABLE MSDSCourseCatalog (
    CID VARCHAR(100),
    CNAME VARCHAR(100) PRIMARY KEY,
    CPREREQ VARCHAR(100)

);

\c msds;

SELECT * FROM MSDSCourseCatalog;
*/