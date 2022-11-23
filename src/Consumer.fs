module Consumer

open System.Threading.Channels
open System.Threading

type Consumer<'T>(count, action) =
  let channel = Channel.CreateUnbounded<'T>()
  let executionCount = ref 0
  let lockObj = obj ()

  let rec worker () =
    let mutable loop = true

    task {
      while loop do
        match! channel.Reader.WaitToReadAsync() with
        | true ->
          match channel.Reader.TryRead() with
          | true, msg ->
            let newExecutionCount = Interlocked.Increment executionCount

            if newExecutionCount % 1000 = 0 then
              printf $"\rExported {newExecutionCount} keys"

            do! action msg
          | _ -> ()
        | _ -> loop <- false
    }

  let workers = Array.init count (ignore >> worker)
  member __.WriteAsync = channel.Writer.WriteAsync
  member _.ExecutionCount = executionCount

  member __.Complete() =
    channel.Writer.Complete()

    for worker in workers do
      worker.Wait()

    channel.Reader.Completion.Wait()
