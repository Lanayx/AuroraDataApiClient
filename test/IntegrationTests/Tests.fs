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
    let dt = DateTime.UtcNow
    let guid = Guid.NewGuid()
    let parameters =
        SqlParameters()
            .Add("timeStampField", dt)
            .Add("textField", "mytext")
            .Add("varCharField", "myvarchar")
            .Add("booleanField", true)
            .Add("uuidField", guid)
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
            Assert.Equal(newId, r.SerialField)
            Assert.True((dt - r.TimeStampField).TotalMilliseconds < 1.0)
            Assert.Equal("mytext", r.TextField)
            Assert.Equal("myvarchar", r.VarCharField)
            Assert.Equal(true, r.BooleanField)
            Assert.Equal(guid, r.UuidField)
            Assert.Equal(Int16.MaxValue, r.SmallintField)
            Assert.Equal(Int32.MaxValue, r.IntegerField)
            Assert.Equal(Int64.MaxValue, r.BigintField)
            Assert.Equal(100.000001m, r.DecimalField)
            Assert.Equal(100.00, r.DoubleField)
            Assert.Equal(100.00f, r.RealField)
            Assert.Equal<byte>(seq { 0uy; 1uy; 2uy }, r.BinaryField.ToArray())
            Assert.Null(r.NullIntField)
            Assert.Equal<string>(seq{"one"; "two"; "three"}, r.StringArrayField)
            Assert.Equal({ Id = 1; Name = "James" }, r.JsonField)
            Assert.Equal({ Id = 2; Name = "John" }, r.JsonbField)
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
    let dt = DateTime.UtcNow
    let parameters =
        SqlParameters()
            .Add("timeStampField", dt)
            .Add("dateTimeField", dt)
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
            Assert.Equal(newId, int64 r.SerialField)
            Assert.True((dt - r.TimeStampField).TotalMilliseconds < 1.0)
            Assert.True((dt - r.DateTimeField).TotalMilliseconds < 1.0)
            Assert.Equal("mytext", r.TextField)
            Assert.Equal("myvarchar", r.VarCharField)
            Assert.Equal(true, r.BooleanField)
            Assert.Equal(SByte.MaxValue, r.TinyintField)
            Assert.Equal(Int16.MaxValue, r.SmallintField)
            Assert.Equal(Int32.MaxValue, r.IntegerField)
            Assert.Equal(Int64.MaxValue, r.BigintField)
            Assert.Equal(100.000001m, r.DecimalField)
            Assert.Equal(100.00, r.DoubleField)
            Assert.Equal(100.00f, r.FloatField)
            Assert.Equal<byte>(seq { 0uy; 1uy; 2uy }, r.BinaryField.ToArray())
            Assert.Null(r.NullIntField)
            Assert.Equal({ Id = 1; Name = "James" }, r.JsonField)
        | ValueNone ->
            failwith "Record should not be null"
                    
    }
    