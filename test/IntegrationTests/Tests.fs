module IntegrationTests.Tests

open System
open System.IO
open Amazon.RDSDataService
open AuroraDataApiClient
open Xunit

let getClient engineType rdsClient =
    AuroraClient({
        RdsDataServiceClient = rdsClient
        SecretArn = MyData.secretArn
        AuroraArn = MyData.serverArn
        DatabaseName = MyData.databaseName
        EngineType = engineType
    })
[<CLIMutable>]
type Person = {
    Id: int
    Name: string
}
[<CLIMutable>]
type TestPostgreSqlRecord = {
    SerialField: int
    TimeStampField: DateTime
    TextField: string
    VarCharField: string
    BooleanField: bool
    UuidField: Guid
    SmallintField: int16
    IntegerField: int32
    BigintField: int64
    DecimalField: decimal
    DoubleField: float
    RealField: single
    BinaryField: MemoryStream
    NullIntField: Nullable<int>
    StringArrayField: ResizeArray<string>
    JsonField: Person
    JsonbField: Person
}

[<CLIMutable>]
type TestMySqlRecord = {
    SerialField: uint64
    TimeStampField: DateTime
    DateTimeField: DateTime
    TextField: string
    VarCharField: string
    BooleanField: bool
    TinyintField: int8
    SmallintField: int16
    IntegerField: int32
    BigintField: int64
    DecimalField: decimal
    DoubleField: float
    FloatField: single
    BinaryField: MemoryStream
    NullIntField: Nullable<int>
    JsonField: Person
}

[<Fact>]
let ``Full postgresql test`` () =

    let sql =
        """
        INSERT INTO test (
            "TimeStampField",
            "TextField",
            "VarCharField",
            "BooleanField",
            "UuidField",
            "SmallintField",
            "IntegerField",
            "BigintField",
            "DecimalField",
            "RealField",
            "DoubleField",
            "BinaryField",
            "NullIntField",
            "StringArrayField",
            "JsonField",
            "JsonbField"
        )
        VALUES (
            :timeStampField,
            :textField,
            :varCharField,
            :booleanField,
            :uuidField,
            :smallintField,
            :integerField,
            :bigintField,
            :decimalField,
            :realField,
            :doubleField,
            :binaryField,
            :nullIntField,
            '{"one", "two", "three"}',
            :jsonField,
            :jsonbField
        )
        RETURNING "SerialField"
        """
    let parameters =
        SqlParameters()
            .Add("timeStampField", DateTime.UtcNow)
            .Add("textField", "mytext")
            .Add("varCharField", "myvarchar")
            .Add("booleanField", true)
            .Add("uuidField", Guid.NewGuid())
            .Add("smallintField", Int16.MaxValue)
            .Add("integerField", Int32.MaxValue)
            .Add("bigintField", Int64.MaxValue)
            .Add("decimalField", 100.000001m)
            .Add("realField", 100.00f)
            .Add("doubleField", 100.00)
            .Add("binaryField", new MemoryStream([| 0uy; 1uy; 2uy |]))
            .AddNull("nullIntField")
            .AddJson("jsonField", { Id = 1; Name = "James" })
            .AddJson("jsonbField", { Id = 2; Name = "John" })
    let selectSql =
        """
        SELECT * FROM test
        WHERE "SerialField" = :serialField 
        """
    task {
        use rdsClient =
            new AmazonRDSDataServiceClient(
                MyData.iamAccessKey,
                MyData.iamSecretKey,
                MyData.region
            )
        let client = getClient EngineType.PostgreSql rdsClient
        let! newId = client.ExecuteScalar<int>(sql, parameters)
        let selectParameters =
            SqlParameters()
                .Add("serialField", newId)
        let! newRecord = client.QueryFirst<TestPostgreSqlRecord>(selectSql, selectParameters)
        match newRecord with
        | ValueSome r ->
            ()
        | ValueNone ->
            failwith "Record should not be null"
                    
    }
    
[<Fact>]
let ``Full mysql test`` () =

    let sql =
        """
        INSERT INTO test (
            `TimeStampField`,
            `DateTimeField`,
            `TextField`,
            `VarCharField`,
            `BooleanField`,
            `TinyintField`,
            `SmallintField`,
            `IntegerField`,
            `BigintField`,
            `DecimalField`,
            `FloatField`,
            `DoubleField`,
            `BinaryField`,
            `NullIntField`,
            `JsonField`
        )
        VALUES (
            :timeStampField,
            :dateTimeField,
            :textField,
            :varCharField,
            :booleanField,
            :tinyintField,
            :smallintField,
            :integerField,
            :bigintField,
            :decimalField,
            :floatField,
            :doubleField,
            :binaryField,
            :nullIntField,
            :jsonField
        );
        """
    let parameters =
        SqlParameters()
            .Add("timeStampField", DateTime.UtcNow)
            .Add("dateTimeField", DateTime.UtcNow)
            .Add("textField", "mytext")
            .Add("varCharField", "myvarchar")
            .Add("booleanField", true)
            .Add("tinyintField", SByte.MaxValue)
            .Add("smallintField", Int16.MaxValue)
            .Add("integerField", Int32.MaxValue)
            .Add("bigintField", Int64.MaxValue)
            .Add("decimalField", 100.000001m)
            .Add("floatField", 100.00f)
            .Add("doubleField", 100.00)
            .Add("binaryField", new MemoryStream([| 0uy; 1uy; 2uy |]))
            .AddNull("nullIntField")
            .AddJson("jsonField", { Id = 1; Name = "James" })
    let selectSql =
        """
        SELECT * FROM test
        WHERE `SerialField` = :serialField 
        """
    task {
        use rdsClient =
            new AmazonRDSDataServiceClient(
                MyData.iamAccessKey,
                MyData.iamSecretKey,
                MyData.region
            )
        let client = getClient EngineType.MySql rdsClient
        let! newId = client.ExecuteScalar<int64>(sql, parameters)
        let selectParameters =
            SqlParameters()
                .Add("serialField", newId)
        let! newRecord = client.QueryFirst<TestMySqlRecord>(selectSql, selectParameters)
        match newRecord with
        | ValueSome r ->
            ()
        | ValueNone ->
            failwith "Record should not be null"
                    
    }
    