namespace AuroraDataApiClient

open Amazon.RDSDataService.Model
open System.Threading.Tasks
open System.Runtime.InteropServices

type AuroraClient (settings: AuroraClientSettings) =
    do settings.Validate()
        
    /// Executes query and returns number of records updated
    member this.Execute (sqlCommand,
                         [<Optional;DefaultParameterValue(null:SqlParameters)>] sqlParameters,
                         [<Optional;DefaultParameterValue(null:string)>] transactionId) =
        let request = createExecuteRequest settings sqlCommand sqlParameters false transactionId
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return data.NumberOfRecordsUpdated
        }
        
    /// Executes the query and returns the first column of the first row in the result set
    member this.ExecuteScalar<'T> (sqlCommand,
                                   [<Optional;DefaultParameterValue(null:SqlParameters)>] sqlParameters,
                                   [<Optional;DefaultParameterValue(null:string)>] transactionId): Task<'T> =
        let request = createExecuteRequest settings sqlCommand sqlParameters true transactionId
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return parseScalarData settings.EngineType data
        }        
    
    /// Executes the query and returns records
    member this.Query(sqlCommand,
                      [<Optional;DefaultParameterValue(null:SqlParameters)>] sqlParameters,
                      [<Optional;DefaultParameterValue(null:string)>] transactionId) =
        let request = createExecuteRequest settings sqlCommand sqlParameters true transactionId
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return
                if data.Records.Count = 0 then
                    Seq.empty
                else
                    transformRecords settings.EngineType data
        }
        
    /// Executes the query and returns first record, wrapped in Option
    member this.QueryFirst(sqlCommand,
                           [<Optional;DefaultParameterValue(null:SqlParameters)>] sqlParameters,
                           [<Optional;DefaultParameterValue(null:string)>] transactionId) =
        let request = createExecuteRequest settings sqlCommand sqlParameters true transactionId
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return
                if data.Records.Count = 0 then
                    ValueNone
                else
                    transformRecords settings.EngineType data |> Seq.head |> ValueSome
        }
        
    /// Begins a transaction
    member this.BeginTransaction () =
        let request =
            BeginTransactionRequest (
                SecretArn = settings.SecretArn,
                ResourceArn = settings.AuroraArn,
                Database = settings.DatabaseName
            )
        task {            
            let! response = settings.RdsDataServiceClient.BeginTransactionAsync request
            return response.TransactionId
        }
        
    /// Commits a transaction
    member this.CommitTransaction transactionId =
        let request =
            CommitTransactionRequest (
                SecretArn = settings.SecretArn,
                ResourceArn = settings.AuroraArn,
                TransactionId = transactionId
            )
        task {                
            let! response = settings.RdsDataServiceClient.CommitTransactionAsync request
            return response.TransactionStatus
        }
            
    // Rolls back a transaction
    member this.RollbackTransaction transactionId =
        let request =
            RollbackTransactionRequest (
                SecretArn = settings.SecretArn,
                ResourceArn = settings.AuroraArn,
                TransactionId = transactionId
            )
        task {                
            let! response = settings.RdsDataServiceClient.RollbackTransactionAsync request
            return response.TransactionStatus
        }