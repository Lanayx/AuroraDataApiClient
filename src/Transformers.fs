namespace AuroraDataApiClient

open System
open System.Globalization
open System.Reflection
open System.Text.Json
open Amazon.RDSDataService.Model

[<AutoOpen>]
module internal Transformers =

    let isSetBoolean = lazy typeof<Field>.GetMethod("IsSetBooleanValue", BindingFlags.Instance ||| BindingFlags.NonPublic)
    let isSetLong = lazy typeof<Field>.GetMethod("IsSetLongValue", BindingFlags.Instance ||| BindingFlags.NonPublic)
    let isSetDouble = lazy typeof<Field>.GetMethod("IsSetDoubleValue", BindingFlags.Instance ||| BindingFlags.NonPublic)
    
    let createExecuteRequest (settings: AuroraClientSettings) sqlCommand (parameters: SqlParameters) returnsData transactionId =
        let request =
            ExecuteStatementRequest(
                SecretArn = settings.SecretArn,
                ResourceArn = settings.AuroraArn,
                IncludeResultMetadata = returnsData,
                ContinueAfterTimeout = true,
                Database = settings.DatabaseName,
                Sql = sqlCommand
            )
        if parameters |> isNull |> not then
            parameters.Value
            |> request.Parameters.AddRange
        if transactionId |> isNull |> not then
            request.TransactionId <- transactionId
        request
        
    let getPostgreSqlValue (valueType: Type) (col: ColumnMetadata) (field: Field) =
        match col.TypeName with
        | "text" | "varchar" ->
            field.StringValue |> box |> Ok
        | "bool" ->
            field.BooleanValue |> box |> Ok
        | "uuid" ->
            field.StringValue |> Guid.Parse |> box |> Ok
        | "smallserial" | "int2" ->
            field.LongValue |> int16 |> box |> Ok
        | "serial" | "int4" ->
            field.LongValue |> int32 |> box |> Ok
        | "bigserial" | "int8" ->
            field.LongValue |> box |> Ok
        | "numeric" ->
            field.StringValue |> Decimal.Parse |> box |> Ok
        | "float4" ->
            field.DoubleValue |> single |> box |> Ok
        | "float8" ->
            field.DoubleValue |> box |> Ok
        | "timestamp" ->
            DateTime.Parse(field.StringValue, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal) |> box |> Ok
        | "bytea" ->
            field.BlobValue |> box |> Ok
        | "json" | "jsonb" ->
            JsonSerializer.Deserialize(field.StringValue, valueType) |> box |> Ok
        | "_text" | "_varchar" ->
            field.ArrayValue.StringValues |> box |> Ok
        | "_bigserial" | "_int8" ->
            field.ArrayValue.LongValues |> box |> Ok
        | "_bool" ->
            field.ArrayValue.BooleanValues |> box |> Ok
        | "_float8" ->
            field.ArrayValue.DoubleValues |> box |> Ok
        | _ ->
            Error <| $"Unknown type {col.TypeName} for {col.Name}"
        
    let getMySqlValue (valueType: Type) (col: ColumnMetadata) (field: Field) =
        match col.TypeName with
        | "TEXT" | "VARCHAR" ->
            field.StringValue |> box |> Ok
        | "BIT" ->
            field.BooleanValue |> box |> Ok
        | "TINYINT" ->
            field.LongValue |> int8 |> box |> Ok
        | "SMALLINT" ->
            field.LongValue |> int16 |> box |> Ok
        | "INT" ->
            field.LongValue |> int32 |> box |> Ok
        | "BIGINT" ->
            field.LongValue |> box |> Ok
        | "BIGINT UNSIGNED" ->
            field.LongValue |> uint64 |> box |> Ok
        | "DECIMAL" ->
            field.StringValue |> Decimal.Parse |> box |> Ok
        | "FLOAT" ->
            field.DoubleValue |> single |> box |> Ok
        | "DOUBLE" ->
            field.DoubleValue |> box |> Ok
        | "TIMESTAMP" | "DATETIME" ->
            DateTime.Parse(field.StringValue, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal) |> box |> Ok
        | "BLOB" ->
            field.BlobValue |> box |> Ok
        | "JSON" ->
            JsonSerializer.Deserialize(field.StringValue, valueType) |> box |> Ok
        | _ ->
            Error <| $"Unknown type {col.TypeName} for {col.Name}"
      
    let getValue engineType valueType col field =
        match engineType with
        | EngineType.PostgreSql -> getPostgreSqlValue valueType col field
        | EngineType.MySql -> getMySqlValue valueType col field
        | _ -> failwith "Unknown engine type"
        
    let parse engineType (recordType: Type) (record: ResizeArray<Field>) (columnMetadata: ResizeArray<ColumnMetadata>): 'T =
        let o = Activator.CreateInstance<'T>()
        let mutable errors: ResizeArray<string> = null
        for i in 0..record.Count-1 do
            let field = record.[i]
            let col = columnMetadata.[i]
            let property = recordType.GetProperty col.Name
            if property |> isNull |> not then
                if field.IsNull then
                    property.SetValue(o, null)
                else
                    match getValue engineType property.PropertyType col field with
                    | Ok value ->
                        property.SetValue(o, value)
                    | Error err ->
                        if errors |> isNull then
                            errors <- ResizeArray()
                        errors.Add err
        if errors |> isNull |> not then
            failwith <| String.Join(Environment.NewLine, errors)
        o
        
    let transformRecords engineType (data: ExecuteStatementResponse): 'T seq =
        let t = typeof<'T>
        seq {
            for record in data.Records do
                yield parse engineType t record data.ColumnMetadata
        }
        
    let parseScalarData engineType (data: ExecuteStatementResponse) =
        match engineType with
        | EngineType.PostgreSql ->
            let field =
               if data.Records.Count > 0 then
                   data.Records.[0].[0]
               else
                   failwith "Returned data is empty"
            let column = data.ColumnMetadata.[0]
            match getValue engineType typeof<'T> column field with
            | Ok value ->
                value :?> 'T
            | Error err ->
                failwith err
        | EngineType.MySql ->
            let field =
                if data.GeneratedFields.Count > 0 then
                    data.GeneratedFields.[0]
                else
                   failwith "There are no generated fields found"
            if field.StringValue |> isNull |> not then
                field.StringValue |> box :?> 'T
            elif field.ArrayValue |> isNull |> not then
                field.ArrayValue |> box :?> 'T
            elif field.BlobValue |> isNull |> not then
                field.BlobValue |> box :?> 'T
            elif isSetLong.Value.Invoke(field, null) :?> bool then
                field.LongValue |> box :?> 'T
            elif isSetBoolean.Value.Invoke(field, null) :?> bool then
                field.BooleanValue |> box :?> 'T
            elif isSetDouble.Value.Invoke(field, null) :?> bool then
                field.DoubleValue |> box :?> 'T
            else
                failwith "There are no generated fields found"
        | _ ->
            failwith "Unknown engine type"