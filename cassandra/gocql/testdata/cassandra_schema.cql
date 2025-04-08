CREATE TABLE it_test.AllCqlConstantTypes (
    pk_int int,
    col_double double,
    col_boolean boolean,
    col_varchar text,
    col_float float,
    col_uuid uuid,
    col_blob blob,
    col_map map<text, int>,
    col_set set<int>,
    col_list list<boolean>,
    PRIMARY KEY (pk_int)
);