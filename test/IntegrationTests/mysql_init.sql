CREATE TABLE test
(
    `SerialField`    serial,
    `TimeStampField` timestamp        NOT NULL,
    `DateTimeField`  datetime         NOT NULL,
    `TextField`      text             NOT NULL,
    `VarCharField`   varchar(100)     NOT NULL,
    `BooleanField`   boolean          NOT NULL,
    `TinyintField`   tinyint          NOT NULL,
    `SmallintField`  smallint         NOT NULL,
    `IntegerField`   integer          NOT NULL,
    `BigintField`    bigint           NOT NULL,
    `DecimalField`   decimal          NOT NULL,
    `DoubleField`    double           NOT NULL,
    `FloatField`     float            NOT NULL,
    `BinaryField`    blob             NOT NULL,
    `NullIntField`   int              NULL,
    `JsonField`      json             NOT NULL,
     PRIMARY KEY(`SerialField`)
)
    