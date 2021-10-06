// See https://aka.ms/new-console-template for more information

using System;
using Amazon.RDSDataService;
using AuroraDataApiClient;

using var rdsClient = new AmazonRDSDataServiceClient();
var client = new AuroraClient(new AuroraClientSettings(rdsClient,
    "YOUR-SECRET-ARN", "YOUR-AURORA-CLUSTER-ARN", "YOUR-DATABASE-NAME", EngineType.PostgreSql));

var id = await client.ExecuteScalar<int>(
    "INSERT INTO person (\"Name\") VALUES (:name) RETURNING \"Id\"",
    new SqlParameters().Add("name", "Jim"), null);
var person = await client.QueryFirst<Person>(
    "SELECT \"Id\", \"Name\" FROM person WHERE \"Id\"= :id",
    new SqlParameters().Add("id", id), null);
var result =
    person switch
    {
        { IsValueSome: true } p => p.Value.Name,
        _ => "Person doesn't exist"
    };
Console.WriteLine($"Result: {result}");