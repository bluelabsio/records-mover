CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}"
(
    num int,
    numstr varchar(3),
    str varchar(3),
    comma varchar(1),
    doublequote varchar(1),
    quotecommaquote varchar(3),
    newlinestr varchar(111),
    date date,
    "time" time,
    "timestamp" timestamp,
    "timestamptz" timestamptz
);
