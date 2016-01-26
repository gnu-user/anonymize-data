-- Initial schema
create table raw_data
(
  data JSONB NOT NULL
);

-- Run after load.py
create table individual as select jsonb_array_elements(data) from raw_data;

CREATE OR REPLACE FUNCTION get_random_number(INTEGER, INTEGER) RETURNS INTEGER AS $$
DECLARE
    start_int ALIAS FOR $1;
    end_int ALIAS FOR $2;
BEGIN
    RETURN trunc(random() * (end_int-start_int) + start_int);
END;
$$ LANGUAGE 'plpgsql' STRICT;


CREATE TABLE info AS
SELECT
person->>'insurance_member_id' AS insurance_member_id,
person->>'grocery_member_id' AS grocery_member_id,
person->>'plan_number' AS plan_number,
person->>'name' AS name,
person->>'dob' AS dob,
person->>'address' AS address,
person->>'zip' AS zip,
person->>'credit_card' AS credit_card,
person->>'ad_keywords' AS ad_keywords,
person->>'coupon_code' AS coupon_code
FROM individual;


update info set zip = get_random_number(10000,100000)::text;


CREATE TABLE insurance
(
  insurance_id SERIAL NOT NULL UNIQUE,
  insurance_member_id TEXT,
  plan_number TEXT,
  name TEXT,
  dob TEXT,
  address TEXT,
  zip TEXT,
  PRIMARY KEY (name, dob, address)
);


CREATE TABLE grocery
(
  grocery_id SERIAL NOT NULL UNIQUE,
  grocery_member_id TEXT,
  name TEXT,
  dob TEXT,
  address TEXT,
  credit_card TEXT,
  ad_keywords TEXT,
  coupon_code TEXT,
  PRIMARY KEY (name, dob, address)
);


CREATE TABLE diseases
(
  disease_id SERIAL PRIMARY KEY,
  disease_name TEXT,
  disease_probability NUMERIC
);


CREATE TABLE prescriptions
(
  prescription_id SERIAL PRIMARY KEY,
  disease_treats TEXT,
  chemical_name TEXT,
  marketing_name TEXT,
  prescription_probability NUMERIC
);


CREATE TABLE insurance_health
(
  insurance_health_id SERIAL PRIMARY KEY,
  insurance_id INTEGER NOT NULL REFERENCES insurance(insurance_id),
  disease_id INTEGER REFERENCES diseases(disease_id) DEFAULT NULL,
  prescription_id INTEGER REFERENCES prescriptions(prescription_id) DEFAULT NULL
);


create function distribute (percent NUMERIC)
RETURNS VOID
LANGUAGE SQL
AS
$$
   DELETE FROM insurance;
   DELETE FROM grocery;
   WITH overlap_ins AS
   (
     INSERT INTO insurance (insurance_member_id, plan_number, name, dob, address, zip)
     SELECT insurance_member_id, plan_number, name, dob, address, zip
     FROM info
     ORDER BY random()
     LIMIT (($1/100::numeric) * (SELECT count(*) FROM info))
     RETURNING insurance_member_id
   ),
   overlap_gro AS
   (
     INSERT INTO grocery (grocery_member_id, name, dob, address, credit_card, ad_keywords, coupon_code)
     SELECT grocery_member_id, name, dob, address, credit_card, ad_keywords, coupon_code
     FROM info
     NATURAL JOIN overlap_ins
   ),
   new_insurance AS
   (
     INSERT INTO insurance (insurance_member_id, plan_number, name, dob, address, zip)
     SELECT insurance_member_id, plan_number, name, dob, address, zip
     FROM info
     WHERE insurance_member_id NOT IN (SELECT insurance_member_id FROM overlap_ins)
     ORDER BY random()
     LIMIT (((100-$1)/200::numeric) * (SELECT count(*) FROM info))
     RETURNING insurance_member_id
   )
   INSERT INTO grocery(grocery_member_id, name, dob, address, credit_card, ad_keywords, coupon_code)
   SELECT grocery_member_id, name, dob, address, credit_card, ad_keywords, coupon_code
   FROM info
   WHERE insurance_member_id NOT IN (SELECT insurance_member_id FROM overlap_ins)
   AND insurance_member_id NOT IN (SELECT insurance_member_id FROM new_insurance);
$$;


CREATE FUNCTION assign_health()
RETURNS VOID
LANGUAGE PLPGSQL
AS
$$
  DECLARE
    _insurance_id INTEGER;
    _disease_id INTEGER;
    _disease_name TEXT;
    _disease_prob NUMERIC;
    _prescription_id INTEGER;
    _prescription_prob NUMERIC;
  BEGIN
    FOR _insurance_id IN
      SELECT insurance_id
      FROM insurance
    LOOP
      FOR _disease_id, _disease_name, _disease_prob IN
        SELECT disease_id, disease_name, disease_probability
        FROM diseases
      LOOP
        IF ((SELECT random()) <= _disease_prob) THEN
          FOR _prescription_id, _prescription_prob IN
            SELECT prescription_id, prescription_probability
            FROM prescriptions
            WHERE disease_treats = _disease_name
          LOOP
            IF ((SELECT random()) <= _prescription_prob) THEN
              INSERT INTO insurance_health(insurance_id, disease_id, prescription_id)
              VALUES (_insurance_id, _disease_id, _prescription_id);
            END IF;
          END LOOP;
        END IF;
      END LOOP;
    END LOOP;
  END;
$$


CREATE VIEW insurance_view AS 
  SELECT
    insurance_id,
    insurance_member_id,
    plan_number,
    name,
    dob,
    address,
    zip,
    disease_name,
    chemical_name,
    marketing_name
  FROM
    insurance
  NATURAL JOIN
    insurance_health
  NATURAL JOIN
    diseases
  NATURAL JOIN
    prescriptions;


CREATE VIEW matches AS
  SELECT *
  FROM insurance 
  JOIN grocery USING (name, dob, address);