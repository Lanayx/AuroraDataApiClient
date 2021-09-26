﻿namespace AuroraDataApiClient

open System
open System.Globalization
open System.Text.Json
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
    member this.Add(name: string, value: decimal) =
        SqlParameter(
            Name = name,
            Value = Field(StringValue = value.ToString(CultureInfo.InvariantCulture)),
            TypeHint = TypeHint.DECIMAL
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: Guid) =
        SqlParameter(
            Name = name,
            Value = Field(StringValue = value.ToString()),
            TypeHint = TypeHint.UUID
        ) |> allParameters.Add
        this
    member this.AddJson<'T>(name: string, value: 'T) =
        SqlParameter(
            Name = name,
            Value = Field(StringValue = JsonSerializer.Serialize(value)),
            TypeHint = TypeHint.JSON
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


[<AutoOpen>]
module internal TransformHelpers =
    let createExecuteRequest (settings: AuroraClientSettings) sqlCommand (parameters: SqlParameters) returnsData =
        let request =
            ExecuteStatementRequest(
                SecretArn = settings.SecretArn,
                ResourceArn = settings.AuroraServerArn,
                IncludeResultMetadata = returnsData,
                ContinueAfterTimeout = true,
                Database = settings.DatabaseName,
                Sql = sqlCommand
            )
        if parameters |> isNull |> not then
            parameters.Value
            |> request.Parameters.AddRange
        request
        
    let getValue (col: ColumnMetadata) field =
        //TODO
        0
        
        
    let parse (data: (ColumnMetadata*Field) seq): 'T =
        let t = typeof<'T>
        let o = Activator.CreateInstance<'T>()
        for col, field in data do
            let value = getValue col field
            t.GetProperty(col.Name).SetValue(o, value)
        o
        
    let transformRecords (data: ExecuteStatementResponse) =                  
        data.Records
        |> Seq.map (fun record ->
            record
            |> Seq.zip data.ColumnMetadata
            |> parse               
            )


type AuroraClient (settings: AuroraClientSettings) =
    do settings.Validate()
        
    member this.ExecuteSql (sqlCommand, sqlParameters) =
        let request = createExecuteRequest settings sqlCommand sqlParameters false
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return data.NumberOfRecordsUpdated
        }
        
    member this.BeginTransaction () =
        let request =
            BeginTransactionRequest (
                SecretArn = settings.SecretArn,
                ResourceArn = settings.AuroraServerArn,
                Database = settings.DatabaseName
            )
        task {            
            let! response = settings.RdsDataServiceClient.BeginTransactionAsync request
            return response.TransactionId
        }
        
        member this.CommitTransaction transactionId =
            let request =
                CommitTransactionRequest (
                    SecretArn = settings.SecretArn,
                    ResourceArn = settings.AuroraServerArn,
                    TransactionId = transactionId
                )
            task {                
                let! response = settings.RdsDataServiceClient.CommitTransactionAsync request
                return response.TransactionStatus
            }
            
        member this.RollbackTransaction transactionId =
            let request =
                RollbackTransactionRequest (
                    SecretArn = settings.SecretArn,
                    ResourceArn = settings.AuroraServerArn,
                    TransactionId = transactionId
                )
            task {                
                let! response = settings.RdsDataServiceClient.RollbackTransactionAsync request
                return response.TransactionStatus
            }

        member this.GetRows(sqlCommand, sqlParameters) =
            let request = createExecuteRequest settings sqlCommand sqlParameters true
            task {
                let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
                return
                    if data.Records.Count > 0 then
                        Seq.empty
                    else
                        transformRecords data
            }