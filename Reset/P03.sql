/*
Group #73
1. San Deigo John Michael Bautista
  - Triggers
  - Contribution B
2. Joshua Wee Ming-En
  - Triggers
  - Contribution B
3. Maximilliano Utomo Quok
    - Functions
4. Liu Zhengyang
    - Procedures
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
      (NEW.fromdate >= h.fromdate AND NEW.fromdate <= h.todate)
      OR (NEW.todate >= h.fromdate AND NEW.todate <= h.todate)
      OR (NEW.fromdate <= h.todate AND NEW.todate >= h.todate)
    )) INTO is_overlap_exists;

  RAISE NOTICE 'overlap: %', is_overlap_exists;

  IF is_overlap_exists THEN
    RAISE EXCEPTION 'Drivers cannot be double-booked.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_driver_not_double_booked
BEFORE INSERT ON Hires
FOR EACH ROW EXECUTE FUNCTION check_driver_not_double_booked_func();

-- TRIGGER 2. Cars (ie CarDetails) cannot be double-booked.✅

CREATE OR REPLACE FUNCTION check_car_not_double_booked_func() RETURNS TRIGGER AS $$
DECLARE
    overlapping_count INT;
BEGIN
  SELECT COUNT(*) INTO overlapping_count
  FROM Assigns a, Bookings b, Bookings new_b
  WHERE a.bid = b.bid AND a.plate = NEW.plate AND NEW.bid = new_b.bid AND a.plate = NEW.plate
  AND b.sdate < new_b.sdate + new_b.days --checks if START of existing booking(b.sdate) is earlier than END of new assignment(new_b.edate), 
  AND b.sdate + b.days > new_b.sdate;   --checks if END of existing booking(b.edate) is later than START of new assignment(new_b.sdate)

  IF overlapping_count > 0 THEN
        RAISE EXCEPTION 'Car is already booked for another assignment during the specified period'; --decide to raise or fail silently, BOTH OK
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER prevent_car_double_booking 
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_not_double_booked_func();

-- TRIGGER 3. During handover, the employee must be located in the same location the booking is for.✅

CREATE OR REPLACE FUNCTION check_employee_location() RETURNS TRIGGER AS $$
DECLARE 
    employee_zip INT;
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
    RAISE EXCEPTION 'Employee must be located in the same location as the booking;';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER enforce_employee_location 
BEFORE INSERT ON Handover
FOR EACH ROW EXECUTE FUNCTION check_employee_location();

-- TRIGGER 4. The car assigned to the booking must be for the car models for the booking.✅

CREATE OR REPLACE FUNCTION check_car_model_assignment_func() RETURNS TRIGGER AS $$
BEGIN
  -- Check if the assigned car has the same brand and model as the booking
  IF (SELECT brand || ' ' || model FROM CarDetails WHERE plate = NEW.plate) <> (SELECT brand || ' ' || model FROM Bookings WHERE bid = NEW.bid) THEN
    RAISE EXCEPTION 'The assigned car must have the same brand and model as the booking.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_car_model_assignment 
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_model_assignment_func();

--TRIGGER 5. The car (i.e., CarDetails) assigned to the booking must be parked in the same location as the booking is for.✅

CREATE OR REPLACE FUNCTION check_car_location_assignment_func() RETURNS TRIGGER AS $$
BEGIN
  -- Check if the assigned car is parked in the same location as the booking
  IF (SELECT zip FROM CarDetails WHERE plate = NEW.plate) <> (SELECT zip FROM Bookings WHERE bid = NEW.bid) THEN
    RAISE EXCEPTION 'The assigned car must be parked in the same location as the booking.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_car_location_assignment
BEFORE INSERT ON Assigns
FOR EACH ROW EXECUTE FUNCTION check_car_location_assignment_func();

-- TRIGGER 6. Drivers must be hired within the start date and end date of a booking.✅

CREATE OR REPLACE FUNCTION check_driver_hiring_date_func() RETURNS TRIGGER AS $$
DECLARE
    booking_start_date DATE; 
    booking_end_date DATE;  --new.fromdate = 0103, new.todate = 0120
BEGIN
  SELECT sdate, sdate + days INTO booking_start_date, booking_end_date 
  FROM Bookings --sdate = 0103, edate = 0105
  WHERE bid = NEW.bid;

  -- Check if the hiring date of the driver overlaps with the booking schedule
  IF (NEW.todate > booking_end_date) OR (NEW.fromdate < booking_start_date) THEN
    RAISE EXCEPTION 'Drivers must be hired within the start date and end date of a booking.';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_driver_hiring_date
BEFORE INSERT ON Hires
FOR EACH ROW EXECUTE FUNCTION check_driver_hiring_date_func();

--PROCEDURE 1 Add Employees ✅
CREATE OR REPLACE PROCEDURE add_employees (
    eids INT[], 
    enames TEXT[], 
    ephones INT[], 
    zips INT[], 
    pdvls TEXT[] 
) AS $$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..array_length(eids, 1)
    LOOP
        INSERT INTO Employees (eid, ename, ephone, zip) 
        VALUES (eids[i], enames[i], ephones[i], zips[i]);

        IF pdvls[i] IS NOT NULL THEN
            INSERT INTO Drivers (eid, pdvl) 
            VALUES (eids[i], pdvls[i]);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

--PROCEDURE 2 Add Car Model ✅
CREATE OR REPLACE PROCEDURE add_car (
    brand TEXT , model TEXT , capacity INT,
    deposit NUMERIC , daily NUMERIC ,
    plates TEXT[] , colors TEXT[] , pyears INT[], zips INT[] ) AS $$
DECLARE
    i INT;
BEGIN
    INSERT INTO CarModels (brand, model, capacity, deposit, daily) 
    VALUES (brand, model, capacity, deposit, daily);

    IF array_length(plates, 1) IS NULL THEN
        RETURN;
    END IF;

    FOR i IN 1..array_length(plates, 1) LOOP
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
CREATE OR REPLACE PROCEDURE auto_assign() AS $$
DECLARE
    booking_record RECORD;
    car_details_record RECORD;
BEGIN
    FOR booking_record IN (
        SELECT * FROM Bookings 
        WHERE bid NOT IN (SELECT bid FROM Assigns) 
        ORDER BY bid ASC
    ) LOOP
        FOR car_details_record IN (
            SELECT * FROM CarDetails 
            WHERE brand = booking_record.brand AND model = booking_record.model AND zip = booking_record.zip 
            AND plate NOT IN (
                SELECT plate FROM Assigns WHERE bid IN (
                    SELECT bid FROM Bookings WHERE (sdate BETWEEN booking_record.sdate AND booking_record.sdate + booking_record.days) OR 
                    (sdate + days BETWEEN booking_record.sdate AND booking_record.sdate + booking_record.days)
                )
            ) 
            ORDER BY plate ASC
        ) LOOP
            INSERT INTO Assigns (bid, plate) VALUES (booking_record.bid, car_details_record.plate);
            EXIT; -- Exit the loop once a car is assigned
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION 1 ## Checked, passes ProjectChecker.py testcases (had to use psql shell one by one) ✅
CREATE OR REPLACE FUNCTION compute_revenue (sdate DATE, edate DATE) RETURNS NUMERIC AS $$
DECLARE
  booking_revenue NUMERIC;
  driver_revenue NUMERIC;
  car_cost NUMERIC;
BEGIN
  -- Compute booking revenue
  IF EXISTS (
    SELECT 1
    FROM Bookings B
        JOIN Assigns A ON B.bid = A.bid
        JOIN CarModels CM ON B.model = CM.model
        WHERE B.sdate <= compute_revenue.edate AND B.sdate + B.days > compute_revenue.sdate
  ) THEN
      SELECT SUM(CM.daily * B.days) INTO booking_revenue
      FROM Bookings B
      JOIN CarModels CM ON B.model = CM.model
      JOIN Assigns A ON B.bid = A.bid
      WHERE B.sdate <= compute_revenue.edate AND B.sdate + B.days > compute_revenue.sdate;
    ELSE
        booking_revenue := 0;
    END IF;

  -- Compute driver revenue
  IF EXISTS (
    SELECT 1
    FROM Hires
    WHERE fromdate <= compute_revenue.edate AND todate >= compute_revenue.sdate
    ) THEN
    SELECT SUM((H.todate - H.fromdate + 1) * 10) INTO driver_revenue
    FROM Hires H
    WHERE H.fromdate <= compute_revenue.edate AND H.todate >= compute_revenue.sdate;
    ELSE
    driver_revenue := 0;
    END IF;

  -- Compute car cost
  IF EXISTS (
    SELECT 1
    FROM Assigns A
      JOIN Bookings B ON A.bid = B.bid
      WHERE B.sdate <= compute_revenue.edate AND B.sdate + B.days > compute_revenue.sdate
  ) THEN 
      SELECT COUNT(DISTINCT A.plate) * 100 INTO car_cost
      FROM Assigns A
      JOIN Bookings B ON A.bid = B.bid
      WHERE B.sdate <= compute_revenue.edate AND B.sdate + B.days > compute_revenue.sdate;
  ELSE
    car_cost := 0;
  END IF;

  -- Return total revenue
  RETURN booking_revenue + driver_revenue - car_cost;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION 2 ## Checked, passes ProjectChecker.py testcases (had to use psql shell one by one) ✅
CREATE OR REPLACE FUNCTION top_n_location(n INT, sdate DATE, edate DATE) 
RETURNS TABLE(lname TEXT, revenue NUMERIC, rank INT) AS $$
BEGIN
    RETURN QUERY (
        WITH location_revenue AS (
            SELECT L.lname AS lname,
            SUM(CASE WHEN B.sdate <= top_n_location.edate AND B.sdate + B.days > top_n_location.sdate THEN (B.days * CM.daily) ELSE 0 END) +
            SUM(CASE WHEN H.fromdate <= top_n_location.edate AND H.todate >= top_n_location.sdate THEN (H.todate - H.fromdate + 1) * 10 ELSE 0 END) -
            100 * (
                SELECT COALESCE(COUNT(DISTINCT A.plate), 0)
                FROM Assigns A
                JOIN Bookings B ON A.bid = B.bid
                WHERE B.sdate <= top_n_location.edate AND B.sdate + B.days > top_n_location.sdate AND B.zip = L.zip
            ) AS revenue
            FROM Locations L
            JOIN Bookings B ON B.zip = L.zip
            JOIN Assigns A ON A.bid = B.bid
            JOIN CarModels CM ON B.brand = CM.brand AND B.model = CM.model
            LEFT JOIN Hires H ON A.bid = H.bid
            GROUP BY L.lname, L.zip
        ),
        location_ranks AS (
            SELECT
        location_revenue.lname,
        location_revenue.revenue,
        COUNT(*) OVER (ORDER BY location_revenue.revenue DESC RANGE UNBOUNDED PRECEDING)::INT AS rank
      FROM location_revenue
        )
        SELECT location_ranks.lname, location_ranks.revenue, location_ranks.rank
        FROM location_ranks
        WHERE location_ranks.rank <= top_n_location.n
        ORDER BY location_ranks.rank, location_ranks.lname
    );
END;
$$ LANGUAGE plpgsql;