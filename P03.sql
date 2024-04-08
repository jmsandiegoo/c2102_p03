/*
Group #73
1. San Deigo John Michael Bautista
  - Triggers
  - Contribution B
2. Joshua Wee Ming-En
  - Triggers
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
    RAISE EXCEPTION 'Employee must be located in the same location as the booking;'
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER 4. The car assigned to the booking must be for the car models for the booking.
CREATE TRIGGER check_car_model_assignment
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_model_assignment_func();

CREATE OR REPLACE FUNCTION check_car_model_assignment_func() RETURNS TRIGGER AS $$
BEGIN
  -- Check if the assigned car has the same brand and model as the booking
  IF (SELECT brand || ' ' || model FROM CarDetails WHERE plate = NEW.plate) <> (SELECT brand || ' ' || model FROM Bookings WHERE bid = NEW.bid) THEN
    RAISE EXCEPTION 'The assigned car must have the same brand and model as the booking.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--TRIGGER 5. The car (i.e., CarDetails) assigned to the booking must be parked in the same location as the booking is for.
CREATE TRIGGER check_car_location_assignment
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_location_assignment_func();

CREATE OR REPLACE FUNCTION check_car_location_assignment_func() RETURNS TRIGGER AS $$
BEGIN
  -- Check if the assigned car is parked in the same location as the booking
  IF (SELECT zip FROM CarDetails WHERE plate = NEW.plate) <> (SELECT zip FROM Bookings WHERE bid = NEW.bid) THEN
    RAISE EXCEPTION 'The assigned car must be parked in the same location as the booking.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER 6. Drivers must be hired within the start date and end date of a booking.
CREATE TRIGGER check_driver_hiring_date
BEFORE INSERT ON Hires
FOR EACH ROW EXECUTE FUNCTION check_driver_hiring_date_func();

CREATE OR REPLACE FUNCTION check_driver_hiring_date_func() RETURNS TRIGGER AS $$
BEGIN
  DECLARE
      booking_start_date DATE;
      booking_end_date DATE;
  BEGIN
      SELECT sdate, sdate + days INTO booking_start_date, booking_end_date
      FROM Bookings
      WHERE bid = NEW.bid;

  -- Check if the hiring date of the driver overlaps with the booking schedule
    IF (NEW.fromdate > booking_end_date) OR (NEW.todate < booking_start_date) THEN
      RAISE EXCEPTION 'Drivers must be hired within the start date and end date of a booking.';
    END IF;
  END

  RETURN NEW;
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
