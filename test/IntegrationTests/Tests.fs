module IntegrationTests.Tests

open System
open Amazon.RDSDataService
open AuroraDataApiClient
open Xunit

let getClient rdsClient =
    AuroraClient({
        RdsDataServiceClient = rdsClient
        SecretArn = MyData.secretArn
        AuroraServerArn = MyData.serverArn
        DatabaseName = MyData.databaseName
    })

[<Fact>]
let ``Full postgresql test`` () =
    use rdsClient =
        new AmazonRDSDataServiceClient(
            MyData.iamAccessKey,
            MyData.iamSecretKey,
            MyData.region
        )
    let client = getClient rdsClient
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
            "NumericField",
            "DecimalField",
            "RealField",
            "DoubleField",
            "NullIntField"
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
            :numericField,
            :decimalField,
            :realField,
            :doubleField,
            :nullIntField
        )
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
            .Add("numericField", 100.000001m)
            .Add("decimalField", 100.000001m)
            .Add("realField", 100.00f)
            .Add("doubleField", 100.00)
            .Add("nullIntField", 123)
    
    client.ExecuteSql(sql, parameters).Wait()