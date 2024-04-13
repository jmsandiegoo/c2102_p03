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

-- TRIGGER 1. Drivers cannot be double-booked. tested/working: ✅
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

CREATE OR REPLACE TRIGGER check_driver_not_double_booked
BEFORE INSERT ON Hires
FOR EACH ROW EXECUTE FUNCTION check_driver_not_double_booked_func();

-- TRIGGER 2. (TODO) Cars (ie CarDetails) cannot be double-booked.
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

CREATE OR REPLACE TRIGGER prevent_car_double_booking
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_not_double_booked_func();

-- TRIGGER 3. (TODO) During handover, the employee must be located in the same location the booking is for.
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

CREATE OR REPLACE TRIGGER enforce_employee_location
BEFORE INSERT ON Handover
FOR EACH ROW EXECUTE FUNCTION check_employee_location();

-- TRIGGER 4. Car Details assigned to booking must be same as 
-- the car models in that booking. tested/working: ✅
CREATE OR REPLACE FUNCTION check_car_details_models_same_func()
RETURNS TRIGGER AS $$
DECLARE
  is_same BOOLEAN;
  assign_brand TEXT;
  assign_model TEXT;
BEGIN
  -- get the (brand, model) of the plate
  SELECT cd.brand, cd.model INTO assign_brand, assign_model
  FROM CarDetails cd
  WHERE cd.plate = NEW.plate;

  IF NOT FOUND THEN
    RAISE NOTICE 'Car Details w/ plate % could not be found', NEW.plate;
    RETURN NULL;
  END IF;

  -- check if it is assigned model, brand is the same with the respective bookings
  SELECT EXISTS (
    SELECT 1
    FROM Bookings b
    WHERE b.bid = NEW.bid
    AND b.brand = assign_brand
    AND b.model = assign_model
  ) INTO is_same;

  IF NOT is_same THEN
    RAISE NOTICE 'Assign car model is not the same as the booking car model';
    RETURN NULL;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_car_details_models_same
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_details_models_same_func();

-- TRIGGER 5. Assigned car details to booking must be parked at same location as booking for.
-- test/working: ✅
CREATE OR REPLACE FUNCTION check_car_parked_same_loc_func()
RETURNS TRIGGER AS $$
DECLARE
  assign_zip INT;
  booking_zip INT;
BEGIN
  -- get the zip of the car assigned
  SELECT cd.zip INTO assign_zip
  FROM CarDetails cd
  WHERE cd.plate = NEW.plate;

  IF NOT FOUND THEN
    RAISE NOTICE 'Car Details w/ plate % could not be found', NEW.plate;
    RETURN NULL;
  END IF;

  -- get the zip of the booking
  SELECT b.zip INTO booking_zip
  FROM Bookings b
  WHERE b.bid = NEW.bid;

  IF NOT FOUND THEN
    RAISE NOTICE 'Booking w/ bid % could not be found', NEW.bid;
    RETURN NULL;
  END IF;

  IF assign_zip <> booking_zip THEN
    RAISE NOTICE 'Car assigned not parked at the booking zip location';
    RETURN NULL;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_car_parked_same_loc
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_parked_same_loc_func();

/*
  Write your Routines Below
    Comment out your routine if you cannot complete
    the routine.
    If any of your routine causes error (even those
    that are incomplete), you may get 0 mark for P03.
*/

-- PROCEDURE 1 test/working: ✅
CREATE OR REPLACE PROCEDURE add_employees (
  eids INT[], enames TEXT[], ephones INT[], zips INT[], pdvls TEXT[]
) AS $$
BEGIN
  FOR i IN 1 .. array_upper(eids, 1) LOOP
    INSERT INTO Employees (eid, ename, ephone, zip)
    VALUES (eids[i], enames[i], ephones[i], zips[i]);

    IF pdvls[i] IS NULL THEN
      CONTINUE;
    END IF;

    INSERT INTO Drivers (eid, pdvl)
    VALUES (eids[i], pdvls[i]);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- PROCEDURE 2 test/working: ✅
CREATE OR REPLACE PROCEDURE add_car (
  brand   TEXT   , model  TEXT   , capacity INT  ,
  deposit NUMERIC, daily  NUMERIC,
  plates  TEXT[] , colors TEXT[] , pyears   INT[], zips INT[]
) AS $$
BEGIN
  INSERT INTO CarModels (brand, model, capacity, deposit, daily)
  VALUES (brand, model, capacity, deposit, daily);

  FOR i IN 1 .. array_upper(plates, 1) LOOP
      INSERT INTO CarDetails (plate, color, pyear, brand, model, zip) 
      VALUES (plates[i], colors[i], pyears[i], brand, model, zips[i]);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- PROCEDURE 3 test/working: ✅
CREATE OR REPLACE PROCEDURE return_car (
  p_bid INT, p_eid INT
) AS $$
DECLARE
  booking_record RECORD;
  booking_deposit NUMERIC;
  booking_daily_rate NUMERIC;
  return_cost NUMERIC;
  return_ccnum TEXT;
BEGIN
  -- get the booking record
  SELECT * INTO booking_record
  FROM Bookings b
  WHERE b.bid = p_bid;

  IF NOT FOUND THEN
    RAISE NOTICE 'return_car failed: Booking w/ bid % could not be found', p_bid;
    RETURN;
  END IF;

  -- get the deposit and daily
  SELECT cm.deposit, cm.daily INTO booking_deposit, booking_daily_rate
  FROM CarModels cm
  WHERE cm.brand = booking_record.brand
  AND cm.model = booking_record.model;

  IF NOT FOUND THEN
    RAISE NOTICE 'return_car failed: Car Model w/ brand % and model % could not be found', booking_record.brand, booking_record.model;
    RETURN;
  END IF;

  -- calculate cost
  return_cost := (booking_daily_rate * booking_record.days) - booking_deposit;

  IF return_cost > 0 THEN
    return_ccnum := booking_record.ccnum;
  END IF;

  INSERT INTO Returned (bid, eid, ccnum, cost)
  VALUES (p_bid, p_eid, return_ccnum, return_cost);
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
