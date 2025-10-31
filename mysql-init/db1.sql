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