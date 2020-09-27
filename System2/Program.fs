open Akka.FSharp
open System

// the most basic configuration of remote actor system
let config = """
akka {
    actor {
        provider = "Akka.Remote.RemoteActorRefProvider, Akka.Remote"
    }
    remote.helios.tcp {
        transport-protocol = tcp
        port = 8777
        hostname = 192.168.0.29
    }
}
"""

[<EntryPoint>]
let main _ =
    System.Console.Title <- "Remote: " + System.Diagnostics.Process.GetCurrentProcess().Id.ToString()
    use system = System.create "RemoteSquares" (Configuration.parse config)
    printfn "Remote Actor %s listening..." system.Name
    System.Console.ReadLine() |> ignore
    0