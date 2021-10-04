CREATE TABLE test
(
    "SerialField"      serial PRIMARY KEY,
    "TimeStampField"   timestamp        NOT NULL,
    "TextField"        text             NOT NULL,
    "VarCharField"     varchar(100)     NOT NULL,
    "BooleanField"     boolean          NOT NULL,
    "UuidField"        uuid             NOT NULL,
    "SmallintField"    smallint         NOT NULL,
    "IntegerField"     integer          NOT NULL,
    "BigintField"      bigint           NOT NULL,
    "DecimalField"     decimal          NOT NULL,
    "DoubleField"      double precision NOT NULL,
    "RealField"        real             NOT NULL,
    "BinaryField"      bytea            NOT NULL,
    "NullIntField"     int              NULL,
    "StringArrayField" text[]           NOT NULL,
    "JsonField"        json             NOT NULL,
    "JsonbField"       jsonb            NOT NULL
)