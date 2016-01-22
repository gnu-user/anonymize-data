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
  insurance_member_id TEXT,
  plan_number TEXT,
  name TEXT,
  dob TEXT,
  address TEXT,
  zip TEXT
);

CREATE TABLE grocery
(
  grocery_member_id TEXT,
  name TEXT,
  dob TEXT,
  address TEXT,
  credit_card TEXT,
  ad_keywords TEXT,
  coupon TEXT
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
     INSERT INTO insurance
     SELECT insurance_member_id, plan_number, name, dob, address, zip
     FROM info
     ORDER BY random()
     LIMIT (($1/100::numeric) * (SELECT count(*) FROM info))
     RETURNING insurance_member_id
   ),
   overlap_gro AS
   (
     INSERT INTO grocery
     SELECT grocery_member_id, name, dob, address, credit_card, ad_keywords, coupon_code
     FROM info
     NATURAL JOIN overlap_ins
   ),
   new_insurance AS
   (
     INSERT INTO insurance
     SELECT insurance_member_id, plan_number, name, dob, address, zip
     FROM info
     WHERE insurance_member_id NOT IN (SELECT insurance_member_id FROM overlap_ins)
     ORDER BY random()
     LIMIT (((100-$1)/200::numeric) * (SELECT count(*) FROM info))
     RETURNING insurance_member_id
   )
   INSERT INTO grocery
   SELECT grocery_member_id, name, dob, address, credit_card, ad_keywords, coupon_code
   FROM info
   WHERE insurance_member_id NOT IN (SELECT insurance_member_id FROM overlap_ins)
   AND insurance_member_id NOT IN (SELECT insurance_member_id FROM new_insurance);
$$;
