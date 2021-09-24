namespace AuroraDataApiClient

open Amazon.RDSDataService
open Amazon.RDSDataService.Model

type AuroraClient(client: AmazonRDSDataServiceClient) =
    
    //let createExecuteRequest sqlCommand parameters =     
    member this.ExecuteSql(sqlCommand, parameters) =
        task {
            let! data = client.ExecuteStatementAsync <| ExecuteStatementRequest()
            return data.NumberOfRecordsUpdated
        }
