namespace AuroraDataApiClient

open Amazon.RDSDataService
open Amazon.RDSDataService.Model

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
    
    let createExecuteRequest sqlCommand parameters =
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
            request.Parameters.AddRange parameters
        request
        
    member this.ExecuteSql(sqlCommand, sqlParameters) =
        task {
            let! data =
                createExecuteRequest sqlCommand sqlParameters
                |> settings.RdsDataServiceClient.ExecuteStatementAsync
            return data.NumberOfRecordsUpdated
        }
