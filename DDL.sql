DROP TABLE IF EXISTS Hires      CASCADE;
DROP TABLE IF EXISTS Returned   CASCADE;
DROP TABLE IF EXISTS Handover   CASCADE;
DROP TABLE IF EXISTS Assigns    CASCADE;
DROP TABLE IF EXISTS Bookings   CASCADE;
DROP TABLE IF EXISTS CarDetails CASCADE;
DROP TABLE IF EXISTS CarModels  CASCADE;
DROP TABLE IF EXISTS Drivers    CASCADE;
DROP TABLE IF EXISTS Employees  CASCADE;
DROP TABLE IF EXISTS Locations  CASCADE;
DROP TABLE IF EXISTS Customers  CASCADE;

CREATE TABLE Customers (
  email     TEXT  PRIMARY KEY,
  dob       DATE  NOT NULL CHECK (dob < NOW()),
  address   TEXT  NOT NULL,
  phone     INT   CHECK (phone >= 80000000 AND phone <= 99999999),
  fsname    TEXT  NOT NULL,
  lsname    TEXT  NOT NULL
);

CREATE TABLE Locations (
  zip       INT   PRIMARY KEY,
  lname     TEXT  NOT NULL UNIQUE,
  laddr     TEXT  NOT NULL
);

CREATE TABLE Employees (
  eid       INT   PRIMARY KEY,
  ename     TEXT  NOT NULL,
  ephone    INT   CHECK (ephone >= 80000000 AND ephone <= 99999999),
  zip       INT   NOT NULL
    REFERENCES Locations (zip)
);

CREATE TABLE Drivers (
  eid       INT   PRIMARY KEY
    REFERENCES Employees (eid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  pdvl      TEXT  NOT NULL UNIQUE
);

CREATE TABLE CarModels (
  brand     TEXT,
  model     TEXT,
  capacity  INT     NOT NULL CHECK (capacity > 0),
  deposit   NUMERIC NOT NULL CHECK (deposit > 0),
  daily     NUMERIC NOT NULL CHECK (daily > 0),
  PRIMARY KEY (brand, model)
);

CREATE TABLE CarDetails (
  plate     TEXT  PRIMARY KEY,
  color     TEXT  NOT NULL,
  pyear     INT   CHECK(pyear > 1900),
  brand     TEXT  NOT NULL,
  model     TEXT  NOT NULL,
  zip       INT   NOT NULL
    REFERENCES Locations (zip),
  FOREIGN KEY (brand, model) REFERENCES CarModels (brand, model)
);

CREATE TABLE Bookings (
  bid       INT   PRIMARY KEY,
  sdate     DATE  NOT NULL,
  days      INT   NOT NULL CHECK (days > 0),
  email     TEXT  NOT NULL
    REFERENCES Customers (email),
  ccnum     TEXT  NOT NULL,
  bdate     DATE  NOT NULL CHECK (bdate < sdate),
  brand     TEXT  NOT NULL,
  model     TEXT  NOT NULL,
  zip       INT   NOT NULL
    REFERENCES Locations (zip),
  FOREIGN KEY (brand, model) REFERENCES CarModels (brand, model)
);

CREATE TABLE Assigns (
  bid       INT   PRIMARY KEY
    REFERENCES Bookings (bid),
  plate     TEXT  NOT NULL
    REFERENCES CarDetails (plate)
);

CREATE TABLE Handover (
  bid       INT   PRIMARY KEY
    REFERENCES Assigns (bid),
  eid       INT   NOT NULL
    REFERENCES Employees (eid)
);

CREATE TABLE Returned (
  bid       INT   PRIMARY KEY
    REFERENCES Handover (bid),
  eid       INT   NOT NULL
    REFERENCES Employees (eid),
  ccnum     TEXT  CHECK (cost <= 0 OR ccnum IS NOT NULL),
  cost      NUMERIC   NOT NULL
);

CREATE TABLE Hires (
  bid       INT   PRIMARY KEY
    REFERENCES Assigns (bid),
  eid       INT   NOT NULL
    REFERENCES Drivers (eid),
  fromdate  DATE  NOT NULL,
  todate    DATE  NOT NULL CHECK (todate >= fromdate),
  ccnum     TEXT  NOT NULL
);





-- test data

INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');

INSERT INTO Locations (zip, lname, laddr) VALUES
(12345, 'Downtown', '123 City Center'),
(67890, 'Uptown', '456 Suburb Street');

INSERT INTO Employees (eid, ename, ephone, zip) VALUES
(1, 'Alice Smith', 80000001, 12345),
(2, 'Bob Johnson', 80000002, 67890);

INSERT INTO Drivers (eid, pdvl) VALUES
(1, 'PDVL0001'),
(2, 'PDVL0002');

INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Toyota', 'Corolla', 5, 200.00, 50.00),
('Honda', 'Civic', 5, 200.00, 50.00);

INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
('SJD1234', 'Red', 2019, 'Toyota', 'Corolla', 12345),
('GHD5678', 'Blue', 2020, 'Honda', 'Civic', 67890);

INSERT INTO Bookings (bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
(1, '2024-04-10', 7, 'john.doe@example.com', '1111222233334444', '2024-04-01', 'Toyota', 'Corolla', 12345),
(2, '2024-05-15', 5, 'jane.doe@example.com', '5555666677778888', '2024-05-10', 'Honda', 'Civic', 67890),
(3, '2024-06-16', 5, 'jane.doe@example.com', '5555666677778888', '2024-06-09', 'Honda', 'Civic', 67890);

INSERT INTO Assigns (bid, plate) VALUES
(1, 'SJD1234'),
(2, 'GHD5678');

INSERT INTO Handover (bid, eid) VALUES
(1, 1),
(2, 2);

-- Note: Adjust 'ccnum' condition according to your schema's logic
INSERT INTO Returned (bid, eid, ccnum, cost) VALUES
(1, 1, '1111222233334444', 350.00),
(2, 2, '5555666677778888', 250.00);

INSERT INTO Hires (bid, eid, fromdate, todate, ccnum) VALUES
(1, 1, '2024-04-10', '2024-04-17', '1111222233334444'),
(2, 2, '2024-05-15', '2024-05-20', '5555666677778888');

-- trigger 1 double booking test
INSERT INTO Hires (bid, eid, fromdate, todate, ccnum) VALUES
(3, 1, '2024-01-03', '2024-01-05', '1111222233334444'),
(4, 1, '2024-01-05', '2024-01-06', '1111222233334444'),
(5, 1, '2024-01-04', '2024-01-06', '1111222233334444'),
(6, 1, '2024-01-02', '2024-01-06', '1111222233334444'),
(7, 1, '2024-01-06', '2024-01-07', '1111222233334444');

-- TRIGGER 2. Cars (ie CarDetails) cannot be double-booked test
INSERT INTO Locations (zip, lname, laddr) VALUES
(67890, 'Uptown', '456 Suburb Street');
INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Honda', 'Civic', 5, 200.00, 50.00);
INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
('SJD1234', 'blue', '2020', 'Honda', 'Civic', 67890);
INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');
INSERT INTO Bookings( bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
-- (1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
-- (2, '2024-01-05', 1, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);

-- (1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
-- (2, '2024-01-04', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);

-- (1, '2024-01-03', 2 ,'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
-- (2, '2024-01-02', 4 ,'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);

(1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
(2, '2024-01-06', 1, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);

INSERT INTO Assigns (bid, plate) VALUES
(1, 'SJD1234'),
(2, 'SJD1234');

-- TRIGGER 3.  During handover, the employee must be located in the same location the booking is for.
INSERT INTO Locations (zip, lname, laddr) VALUES
(67890, 'Uptown', '456 Suburb Street'),
(12345, 'Downtown', '123 City Center');
INSERT INTO Employees (eid, ename, ephone, zip) VALUES
-- (1, 'Alice Smith', 80000001, 12345),
(1, 'Bob Johnson', 80000002, 67890);
INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');
INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Honda', 'Civic', 5, 200.00, 50.00);
INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
('SJD1234', 'blue', '2020', 'Honda', 'Civic', 67890);
INSERT INTO Bookings( bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
(1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);
-- (2, '2024-01-05', 1, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);
INSERT INTO Assigns (bid, plate) VALUES
(1, 'SJD1234');
-- (2, 'SJD1234');
INSERT INTO Handover (bid, eid) VALUES
(1, 1);

-- trigger 4 test. Car from booking must be same booking car model
INSERT INTO Locations (zip, lname, laddr) VALUES
(67890, 'Uptown', '456 Suburb Street'),
(12345, 'Downtown', '123 City Center');
INSERT INTO Employees (eid, ename, ephone, zip) VALUES
-- (1, 'Alice Smith', 80000001, 12345),
(1, 'Bob Johnson', 80000002, 67890);
INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Honda', 'Civic', 5, 200.00, 50.00),
('Honda', 'FakeCivic', 5, 200.00, 50.00),
('FakeHonda', 'Civic', 5, 200.00, 50.00);
INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
-- ('SJD1234', 'blue', '2020', 'Honda', 'Civic', 67890);
-- ('SJD1234', 'blue', '2020', 'Honda', 'FakeCivic', 67890);
('SJD1234', 'blue', '2020', 'FakeHonda', 'Civic', 67890);
INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');
INSERT INTO Bookings( bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
-- (1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);
(1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'FakeCivic', 67890);
-- (1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'FakeHonda', 'Civic', 67890);
INSERT INTO Assigns (bid, plate) VALUES
(1, 'SJD1234');

-- trigger 5 test. Booking's assigned car same location as booking
INSERT INTO Locations (zip, lname, laddr) VALUES
(67890, 'Uptown', '456 Suburb Street'),
(12345, 'Downtown', '123 City Center');
INSERT INTO Employees (eid, ename, ephone, zip) VALUES
(1, 'Alice Smith', 80000001, 12345),
(2, 'Bob Johnson', 80000002, 67890);
INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Honda', 'Civic', 5, 200.00, 50.00);
INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
('SJD1234', 'blue', '2020', 'Honda', 'Civic', 67890),
('SCD1234', 'blue', '2020', 'Honda', 'Civic', 12345);
INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');
INSERT INTO Bookings( bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
(1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
(2, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 12345);
INSERT INTO Assigns (bid, plate) VALUES
-- (1, 'SJD1234'); --correct
(1, 'SCD1234'); --incorrect


-- trigger 6 test. Drivers hired within booking's sdate,edate 
INSERT INTO Locations (zip, lname, laddr) VALUES
(67890, 'Uptown', '456 Suburb Street'),
(12345, 'Downtown', '123 City Center');
INSERT INTO Employees (eid, ename, ephone, zip) VALUES
(1, 'Alice Smith', 80000001, 12345),
(2, 'Bob Johnson', 80000002, 67890);
INSERT INTO Drivers (eid, pdvl) VALUES
(1, 'PDVL123'),
(2, 'PDVL456');
INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Honda', 'Civic', 5, 200.00, 50.00);
INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
('SJD1234', 'blue', '2020', 'Honda', 'Civic', 67890),
('SCD1234', 'blue', '2020', 'Honda', 'Civic', 12345);
INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');
INSERT INTO Bookings( bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
(1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
(2, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 12345);
INSERT INTO Assigns (bid, plate) VALUES
(1, 'SJD1234');
INSERT INTO Hires (bid, eid, fromdate, todate, ccnum) VALUES
-- (1, 1, '2024-01-03', '2024-01-04', '1111222233334444');
-- (1, 1, '2024-01-03', '2024-01-20', '5555666677778888');
(1, 1, '2024-01-02', '2024-01-04', '5555666677778888');

-- procedure 1 test
CALL add_employees(
  ARRAY[3, 4, 5],
  ARRAY['John Doe', 'Jane Smith', 'Steve Brown'],
  ARRAY[80000003, 80000004, 80000005],
  ARRAY[12345, 12345, 12345]::integer[],
  ARRAY['PDVL123', NULL, 'PDVL456']
);

-- procedure 2 test
CALL add_car(
  'Toyota', 'Prius', 5, 200.00, 50.00, 
  ARRAY['ABC4200', 'ABC4201'], ARRAY['Grey', 'White'], 
  ARRAY[2021, 2022], ARRAY[12345, 67890]);
CALL add_car(
  'Honda', 'Civic', 5, 200.00, 50.00, 
  ARRAY['ABC4200', 'ABC4201'], ARRAY['Grey', 'White'], 
  ARRAY[2021, 2022], ARRAY[12345, 67890]);

-- procedure 3 test. Return car
INSERT INTO Locations (zip, lname, laddr) VALUES
(67890, 'Uptown', '456 Suburb Street'),
(12345, 'Downtown', '123 City Center');
INSERT INTO Employees (eid, ename, ephone, zip) VALUES
(1, 'Alice Smith', 80000001, 67890);
-- INSERT INTO Drivers (eid, pdvl) VALUES
-- (1, 'PDVL123');
INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Honda', 'Civic', 5, 200.00, 50.00);
INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
('SJD1234', 'blue', '2020', 'Honda', 'Civic', 67890);
INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');
INSERT INTO Bookings( bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
(1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);
INSERT INTO Assigns (bid, plate) VALUES
(1, 'SJD1234'); --correct
INSERT INTO Handover (bid, eid) VALUES
(1, 1);

CALL return_car (1, 1);

-- procedure 4 test. Auto assign cars to bookings
-- location, without assigned, brand and model match, no double booking
INSERT INTO Locations (zip, lname, laddr) VALUES
(67890, 'Uptown', '456 Suburb Street'),
(12345, 'Downtown', '123 City Center');
INSERT INTO Employees (eid, ename, ephone, zip) VALUES
(1, 'Alice Smith', 80000001, 67890);
INSERT INTO CarModels (brand, model, capacity, deposit, daily) VALUES
('Honda', 'Civic', 5, 200.00, 50.00),
('FakeHonda', 'Civic', 5, 200.00, 50.00);
INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) VALUES
('BJD1234', 'blue', '2020', 'Honda', 'Civic', 67890),
('AJD1234', 'blue', '2020', 'Honda', 'Civic', 67890);
INSERT INTO Customers (email, dob, address, phone, fsname, lsname) VALUES
('john.doe@example.com', '1985-07-12', '123 Elm Street', 88888888, 'John', 'Doe'),
('jane.doe@example.com', '1990-05-24', '456 Oak Avenue', 87777777, 'Jane', 'Doe');
INSERT INTO Bookings( bid, sdate, days, email, ccnum, bdate, brand, model, zip) VALUES
(1, '2024-01-03', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
(2, '2024-01-03', 20, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
(3, '2024-01-05', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
(4, '2024-01-07', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890),
(5, '2024-01-09', 2, 'jane.doe@example.com', '5555666677778888', '2024-01-01', 'Honda', 'Civic', 67890);
-- INSERT INTO Assigns (bid, plate) VALUES
-- (2, 'BD1234'),
-- (3, 'GJD1234');
CALL auto_assign();
-- INSERT INTO Assigns (bid, plate) VALUES
-- (1, 'SJD1234'); --correct
