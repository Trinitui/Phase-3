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

CREATE TABLE locations (
    neighborhood VARCHAR(100),
    community_area VARCHAR(100),
    zip_code VARCHAR(100)
);

INSERT INTO locations (neighborhood,community_area,zip_code) VALUES 
    ('Rogers Park','1','60626'),
    ('Rogers Park','1','60645'),
    ('Rogers Park','1','60660'),
    ('West Ridge','2','60626'),
    ('West Ridge','2','60645'),
    ('West Ridge','2','60659'),
    ('West Ridge','2','60660'),
    ('Uptown','3','60613'),
    ('Uptown','3','60640'),
    ('Uptown','3','60618')

;

SELECT * FROM locations