open Argu
open StackExchange.Redis
open System.IO
open FSharp.Control

type Arguments =
  | ConnectionString of string
  | Database of int
  | Output of string
  | Parallelism of int

  interface IArgParserTemplate with
    member __.Usage =
      match __ with
      | ConnectionString _ -> "connection string (defaults to localhost)"
      | Database _ -> "database number (defaults to 0)"
      | Output _ -> "output directory (defaults to currentDirectory/database)"
      | Parallelism _ -> "number of parallel tasks (defaults to 1)"

let worker (database: IDatabase) (directory: DirectoryInfo) (key: RedisKey) =
  task {
    let! redisValue = database.StringGetAsync key
    let path = Path.Combine(directory.FullName, key)
    do! File.WriteAllTextAsync(path, redisValue)
  }

[<EntryPoint>]
let main argv =
  let parser = ArgumentParser.Create<Arguments>()

  try
    let results = parser.Parse argv
    let connectionString = results.GetResult(<@ ConnectionString @>, "localhost")
    let database = results.GetResult(<@ Database @>, 0)
    let connectionMultiplexer = ConnectionMultiplexer.Connect connectionString
    let parallelism = results.GetResult(<@ Parallelism @>, 1)

    let output =
      match results.TryGetResult(<@ Output @>) with
      | Some output -> output
      | _ -> Path.Combine(Directory.GetCurrentDirectory(), string database)
      |> Directory.CreateDirectory

    let server =
      connectionMultiplexer.GetEndPoints().[0] |> connectionMultiplexer.GetServer

    let redisDatabase = connectionMultiplexer.GetDatabase database
    let worker = worker redisDatabase output
    let consumer = Consumer.Consumer(parallelism, worker)

    for key in server.Keys database do
      consumer.WriteAsync(key).AsTask().Wait()

    consumer.Complete()
    printf $"\rExported {consumer.ExecutionCount.Value} keys"
  with
  | :? ArguParseException as ex -> printfn $"{ex.Message}"
  | ex -> raise ex

  0
