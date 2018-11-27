open Argu
open StackExchange.Redis
open System.IO
open FSharp.Control

type Arguments =
| ConnectionString of string
| Database of int
| Output of string
with
  interface IArgParserTemplate with
    member __.Usage =
      match __ with
      | ConnectionString _ -> "connection string (defaults to localhost)"
      | Database _ -> "database number (defaults to 0)"
      | Output _ -> "output directory (defaults to currentDirectory/database)"

let stringGetAsync (database : IDatabase) (key : RedisKey) =
  async {
    let! redisValue = database.StringGetAsync key |> Async.AwaitTask
    return string key, string redisValue
  }

let writeAllText (directory : DirectoryInfo) (name, text) =
  async {
    let path = Path.Combine(directory.FullName, name)
    return! File.WriteAllTextAsync(path, text) |> Async.AwaitTask
  }

[<EntryPoint>]
let main argv =
  let parser = ArgumentParser.Create<Arguments>()
  try
    let results = parser.Parse argv
    let connectionString = results.GetResult(<@ ConnectionString  @>, "localhost")
    let database = results.GetResult(<@ Database @>, 0)
    let connectionMultiplexer = ConnectionMultiplexer.Connect connectionString
    let output = 
      match results.TryGetResult(<@ Output @>) with
      | Some output -> output
      | _ -> Path.Combine(Directory.GetCurrentDirectory(), string database) 
      |> Directory.CreateDirectory
    let server = connectionMultiplexer.GetEndPoints().[0] |> connectionMultiplexer.GetServer
    let keys = server.Keys database
    let redisDatabase = connectionMultiplexer.GetDatabase database
    async {
      do!
        keys
        |> AsyncSeq.ofSeq
        |> AsyncSeq.mapAsyncParallel (stringGetAsync redisDatabase)
        |> AsyncSeq.iterAsyncParallel (writeAllText output)
    } 
    |> Async.RunSynchronously
  with
  | :? ArguParseException as ex -> printfn "%s" ex.Message
  | ex -> raise ex
  0