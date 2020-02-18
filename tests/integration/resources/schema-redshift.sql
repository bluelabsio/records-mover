CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}"
(
         "num" INTEGER   ENCODE lzo
         ,"numstr" VARCHAR(3)   ENCODE lzo
         ,"str" VARCHAR(3)   ENCODE lzo
         ,"comma" VARCHAR(1)   ENCODE lzo
         ,"doublequote" VARCHAR(1)   ENCODE lzo
         ,"quotecommaquote" VARCHAR(3)   ENCODE lzo
         ,"newlinestr" VARCHAR(111)   ENCODE lzo
         ,"date" DATE   ENCODE lzo
         ,"time" VARCHAR(8)   ENCODE lzo
         ,"timestamp" TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo
         ,"timestamptz" TIMESTAMP WITH TIME ZONE   ENCODE lzo
 )
 DISTSTYLE EVEN
;
