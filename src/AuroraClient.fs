namespace AuroraDataApiClient

open System
open Amazon.RDSDataService
open Amazon.RDSDataService.Model

[<AllowNullLiteral>]
type SqlParameters() =
    let allParameters = ResizeArray()    
    member this.Add(name: string, value: int) =
        SqlParameter(
            Name = name,
            Value = Field(LongValue = int64 value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: int64) =
        SqlParameter(
            Name = name,
            Value = Field(LongValue = value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: byte) =
        SqlParameter(
            Name = name,
            Value = Field(LongValue = int64 value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: string) =
        SqlParameter(
            Name = name,
            Value = Field(StringValue = value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: single) =
        SqlParameter(
            Name = name,
            Value = Field(DoubleValue = float value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: float) =
        SqlParameter(
            Name = name,
            Value = Field(DoubleValue = value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: DateTime) =
        SqlParameter(
            Name = name,
            Value = Field(StringValue = value.ToString("yyyy-MM-dd hh:mm:ss.fff")),
            TypeHint = TypeHint.TIMESTAMP
        ) |> allParameters.Add
        this
    member this.Value = allParameters

type AuroraClientSettings = {
    RdsDataServiceClient: AmazonRDSDataServiceClient
    SecretArn: string
    AuroraServerArn: string
    DatabaseName: string
} with
    member this.Validate() =
        if isNull this.RdsDataServiceClient then
            invalidArg (nameof(this.RdsDataServiceClient)) "Value can't be null"
        if isNull this.SecretArn then
            invalidArg (nameof(this.SecretArn)) "Value can't be null"
        if isNull this.AuroraServerArn then
            invalidArg (nameof(this.AuroraServerArn)) "Value can't be null"
        if isNull this.DatabaseName then
            invalidArg (nameof(this.DatabaseName)) "Value can't be null"

type AuroraClient(settings: AuroraClientSettings) =
    do settings.Validate()
    
    let createExecuteRequest sqlCommand (parameters: SqlParameters) =
        let request =
            ExecuteStatementRequest(
                SecretArn = settings.SecretArn,
                ResourceArn = settings.AuroraServerArn,
                IncludeResultMetadata = true,
                ContinueAfterTimeout = true,
                Database = settings.DatabaseName,
                Sql = sqlCommand
            )
        if parameters |> isNull |> not then
            parameters.Value
            |> request.Parameters.AddRange
        request
        
    member this.ExecuteSql(sqlCommand, sqlParameters) =
        task {
            let! data =
                createExecuteRequest sqlCommand sqlParameters
                |> settings.RdsDataServiceClient.ExecuteStatementAsync
            return data.NumberOfRecordsUpdated
        }
