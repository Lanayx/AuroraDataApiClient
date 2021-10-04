namespace AuroraDataApiClient

open Amazon.RDSDataService

type EngineType =
    | PostgreSql = 0
    | MySql = 1

type AuroraClientSettings = {
    RdsDataServiceClient: AmazonRDSDataServiceClient
    SecretArn: string
    AuroraArn: string
    DatabaseName: string
    EngineType: EngineType
} with
    member this.Validate() =
        if isNull this.RdsDataServiceClient then
            invalidArg (nameof(this.RdsDataServiceClient)) "Value can't be null"
        if isNull this.SecretArn then
            invalidArg (nameof(this.SecretArn)) "Value can't be null"
        if isNull this.AuroraArn then
            invalidArg (nameof(this.AuroraArn)) "Value can't be null"
        if isNull this.DatabaseName then
            invalidArg (nameof(this.DatabaseName)) "Value can't be null"