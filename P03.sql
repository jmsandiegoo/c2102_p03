/*
Group #73
1. San Deigo John Michael Bautista
  - Triggers
  - Contribution B
2. Name 2
  - Contribution A
  - Contribution B
*/

/* Write your Trigger Below */

-- TRIGGER 1. Drivers cannot be double-booked tested/working: ✅
CREATE TRIGGER check_driver_not_double_booked
BEFORE INSERT ON Hires
FOR EACH ROW EXECUTE FUNCTION check_driver_not_double_booked_func();

CREATE OR REPLACE FUNCTION check_driver_not_double_booked_func()
RETURNS TRIGGER AS $$
DECLARE
  is_overlap_exists BOOLEAN;
BEGIN
  -- query and check for existence of driver overlap bookings
  SELECT EXISTS (
    SELECT 1 FROM Hires h 
    WHERE h.eid = NEW.eid
    AND (
      NEW.fromdate BETWEEN h.fromdate AND h.todate
      OR NEW.todate BETWEEN h.fromdate AND h.todate
      OR h.fromdate BETWEEN NEW.fromdate AND NEW.todate
      OR h.todate BETWEEN NEW.fromdate AND NEW.todate
    )) INTO is_overlap_exists;

  IF is_overlap_exists THEN
    RAISE NOTICE 'Drivers cannot be double-booked.';
    RETURN NULL;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER 2. Cars (ie CarDetails) cannot be double-booked.

CREATE TRIGGER prevent_car_double_booking
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_not_double_booked_func();

CREATE OR REPLACE FUNCTION check_car_not_double_booked_func() RETURNS TRIGGER AS $$
DECLARE
    overlapping_count INT;
BEGIN
  SELECT COUNT(*) INTO overlapping_count
  FROM Assigns a, Bookins b
  WHERE a.bid = b.bid AND a.plate = NEW.plate
  AND b.sdate <= NEW.todate --checks if START of existing booking(b.sdate) is earlier than END of new assignment(NEW.todate), 
  AND b.edate >= NEW.fromdate;   --checks if END of existing booking(b.edate) is later than START of new assignment(NEW.fromdate)

  IF overlapping_count > 0 THEN
        RAISE EXCEPTION 'Car is already booked for another assignment during the specified period'; --decide to raise or fail silently, BOTH OK
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER 3. During handover, the employee must be located in the same location the booking is for.
CREATE TRIGGER enforce_employee_location
BEFORE INSERT ON Handover
FOR EACH ROW EXECUTE FUNCTION check_employee_location();

CREATE OR REPLACE FUNCTION check_employee_location() RETURNS TRIGGER AS $$
DECLARE 
  employee_zip INT
  booking_zip INT;
BEGIN
-- Retrieve zip code of employee
  SELECT zip INTO employee_zip -- select zip as employee_zip?
  FROM Employees
  WHERE eid = NEW.eid;
-- Retrieve zip of booking location
  SELECT zip INTO booking_zip
  FROM Bookings
  WHERE bid = NEW.bid;
--Check if employee's location matches booking's location
IF employee_zip <> booking_zip THEN
  RAISE EXCEPTION 'Employee'










/*
  Write your Routines Below
    Comment out your routine if you cannot complete
    the routine.
    If any of your routine causes error (even those
    that are incomplete), you may get 0 mark for P03.
*/

-- PROCEDURE 1
CREATE OR REPLACE PROCEDURE add_employees (
  eids INT[], enames TEXT[], ephones INT[], zips INT[], pdvls TEXT[]
) AS $$
-- add declarations here
BEGIN
  -- your code here
END;
$$ LANGUAGE plpgsql;


-- PROCEDURE 2
CREATE OR REPLACE PROCEDURE add_car (
  brand   TEXT   , model  TEXT   , capacity INT  ,
  deposit NUMERIC, daily  NUMERIC,
  plates  TEXT[] , colors TEXT[] , pyears   INT[], zips INT[]
) AS $$
-- add declarations here
BEGIN
  -- your code here
END;
$$ LANGUAGE plpgsql;


-- PROCEDURE 3
CREATE OR REPLACE PROCEDURE return_car (
  bid INT, eid INT
) AS $$
-- add declarations here
BEGIN
  -- your code here
END;
$$ LANGUAGE plpgsql;


-- PROCEDURE 4
CREATE OR REPLACE PROCEDURE auto_assign () AS $$
-- add declarations here
BEGIN
  -- your code here
END;
$$ LANGUAGE plpgsql;


-- FUNCTION 1
CREATE OR REPLACE FUNCTION compute_revenue (
  sdate DATE, edate DATE
) RETURNS NUMERIC AS $$
  -- your code here
$$ LANGUAGE plpgsql;


-- FUNCTION 2
CREATE OR REPLACE FUNCTION top_n_location (
  n INT, sdate DATE, edate DATE
) RETURNS TABLE(lname TEXT, revenue NUMERIC, rank INT) AS $$
  -- your code here
$$ LANGUAGE plpgsql;
