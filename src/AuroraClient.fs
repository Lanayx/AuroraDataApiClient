namespace AuroraDataApiClient

open System.Threading.Tasks
open Amazon.RDSDataService.Model

type AuroraClient (settings: AuroraClientSettings) =
    do settings.Validate()
        
    /// Executes query and returns number of records updated
    member this.Execute (sqlCommand, sqlParameters) =
        let request = createExecuteRequest settings sqlCommand sqlParameters false
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return data.NumberOfRecordsUpdated
        }
        
    /// Executes the query and returns the first column of the first row in the result set
    member this.ExecuteScalar<'T> (sqlCommand, sqlParameters): Task<'T> =
        let request = createExecuteRequest settings sqlCommand sqlParameters true
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return parseScalarData settings.EngineType data
        }        
    
    /// Executes the query and returns records
    member this.Query(sqlCommand, sqlParameters) =
        let request = createExecuteRequest settings sqlCommand sqlParameters true
        task {
            let! data = settings.RdsDataServiceClient.ExecuteStatementAsync request
            return
                if data.Records.Count = 0 then
                    Seq.empty
                else
                    transformRecords settings.EngineType data
        }
        
    /// Executes the query and returns first record, wrapped in Option
    member this.QueryFirst(sqlCommand, sqlParameters) =
        let request = createExecuteRequest settings sqlCommand sqlParameters true
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