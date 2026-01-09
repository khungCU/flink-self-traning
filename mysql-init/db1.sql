 USE db_1;
 CREATE TABLE user_1 (
   id INTEGER NOT NULL PRIMARY KEY,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512),
   email VARCHAR(255)
 );
 INSERT INTO user_1 VALUES (110,"user_110","Shanghai","123567891234","user_110@foo.com");

 CREATE TABLE user_2 (
   id INTEGER NOT NULL PRIMARY KEY,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512),
   email VARCHAR(255)
 );
INSERT INTO user_2 VALUES (120,"user_120","Shanghai","123567891234","user_120@foo.com");

/*
Run with root account
docker exec flink-self-traning-mysql-1 mysql -uroot -p123456 -e "
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
  GRANT SELECT ON db_1.* TO 'mysqluser'@'%';
  FLUSH PRIVILEGES;
  SHOW GRANTS FOR 'mysqluser'@'%';
  "
*/

CREATE TABLE shipments (
  shipment_id INT PRIMARY KEY,
  order_id INT,
  origin VARCHAR(255),
  destination VARCHAR(255),
  is_arrived BOOLEAN
);

/*
INSERT INTO shipments VALUES (1, 1,"Shanghai","tokyo", false);
INSERT INTO shipments VALUES (1, 2,"Shanghai","tokyo", false);
UPDATE shipments set is_arrived = true where order_id = 1;
UPDATE shipments set is_arrived = true where order_id = 2;
DELETE FROM shipments WHERE shipment_id = 1;
*/