# DataApi client for Aurora Serverless
*AWS.RDSDataService* wrapper for .NET

[Nuget AuroraDataApiClient](https://www.nuget.org/packages/AuroraDataApiClient)

Allows queries like
```c#
var person = await client.QueryFirst<Person>(
    "SELECT \"Id\", \"Name\" FROM person WHERE \"Id\"= :id",
    new SqlParameters().Add("id", id));
```

Full example can be found in [example folder](https://github.com/Lanayx/AuroraDataApiClient/tree/main/example)

