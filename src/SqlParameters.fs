namespace AuroraDataApiClient

open System
open System.Globalization
open System.IO
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
    member this.Add(name: string, value: int8) =
        SqlParameter(
            Name = name,
            Value = Field(LongValue = int64 value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: int16) =
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
    member this.Add(name: string, value: bool) =
        SqlParameter(
            Name = name,
            Value = Field(BooleanValue = value)
        ) |> allParameters.Add
        this
    member this.Add(name: string, value: DateTime) =
        SqlParameter(
            Name = name,
            Value = Field(StringValue = value.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)),
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
    member this.Add(name: string, value: MemoryStream) =
        SqlParameter(
            Name = name,
            Value = Field(BlobValue = value)
        ) |> allParameters.Add
        this
    member this.AddJson(name: string, value: 'T) =
        SqlParameter(
            Name = name,
            Value = Field(StringValue = JsonSerializer.Serialize value),
            TypeHint = TypeHint.JSON
        ) |> allParameters.Add
        this
    member this.AddNull(name: string) =
        SqlParameter(
            Name = name,
            Value = Field(IsNull = true)
        ) |> allParameters.Add
        this

    member this.Value = allParameters

