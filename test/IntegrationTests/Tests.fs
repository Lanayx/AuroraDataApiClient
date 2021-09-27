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
            MyData.iamSecretKey
        )
    let client = getClient rdsClient
    client.ExecuteSql("TODO: insert data", null) |> ignore
    client.GetRows("TODO: receive data", null) |> ignore