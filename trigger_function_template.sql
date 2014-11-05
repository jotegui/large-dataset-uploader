CREATE OR REPLACE FUNCTION {1}_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    {0}
    ELSE
        RAISE EXCEPTION 'Species out of range.  Fix the {1}_insert_trigger() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;
