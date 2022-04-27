-- pet birth dates sql script
SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC {{ params.begin_date }} AND {{ params.end_date }};

