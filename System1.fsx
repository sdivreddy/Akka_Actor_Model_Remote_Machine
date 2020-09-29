#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Threading
open Akka.TestKit

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = 8777
                hostname = 192.168.0.29
            }
        }")

let system = ActorSystem.Create("Squares", configuration)
type Information = 
    | Info of (int64*int64*int64)
    | Done of (string)
    | Input of (int64*int64)

// Printer Actor - To print the output
let printerActor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let boss = system.ActorSelection("akka.tcp://Squares@192.168.0.29:8777/user/boss")
        let! (message:obj) = mailbox.Receive()
        if (message :? string) then
            boss <! Done("Done Remote")
        else  
            let (value:int64) = downcast message
            printfn "%d" value      
        return! loop()
    }
    loop()
let printerRef = spawn system "Printer" printerActor

// Worker Actors - Takes input from Boss and do the processing using sliding window algo and returns the completed message.
let WorkerActor (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! (message : Information) = mailbox.Receive()
        let boss = mailbox.Sender()
        let x : Information = message
        match x with
        | Info(startind, k, endind) -> 
            let mutable sum = 0L
            for i in startind .. startind+k-1L do
                sum <- sum + (i*i)
            let value = sum |> double |> sqrt |> int64
            if sum = (value * value) then
                printerRef <! startind
            for i in startind+1L .. endind do
                sum <- sum + ((i+k-1L)*(i+k-1L)) - ((i-1L)*(i-1L))
                let value = sum |> double |> sqrt |> int64
                if sum = (value * value) then
                    printerRef <! i
            boss <! Done("Done Client")

        return! loop()
    }
    loop()

// Boss - Takes input from command line and spawns the actor pool for Local System Actors. 
// Splits the tasks based on cores count and allocates using Round-Robin in local system and 
// sends the subproblems to the remote boss which inturn allocates to it's child actors in the remote machine
let BossActor (mailbox:Actor<_>) = 
    let sref = system.ActorSelection(
                    "akka.tcp://RemoteSquares@192.168.0.88:8778/user/RemoteBossActor")

    let actcount = System.Environment.ProcessorCount |> int64
    let totalactors = actcount*250L
    let workerActorsPool = 
            [1L .. totalactors]
            |> List.map(fun id -> spawn system (sprintf "Local_%d" id) WorkerActor)
    let workerenum = [|for lp in workerActorsPool -> lp|]
    let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))

    let mutable completed = 0L
    let splits = totalactors*2L

    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with 
        | Input(n, k) -> 
            let mutable startind = 1L
            let subsize = n/splits
            for i in [1L..splits] do
                // if i = splits then
                //     sref <! (startind, k, n) 
                // if i < splits/2L then
                //     let endind = startind + subsize - 1L
                //     workerSystem <! Info(startind, k, endind)
                //     startind <- startind + subsize  
                // else
                //     sref <! (startind, k, (startind + subsize - 1L))
                //     startind <- startind + subsize                                  
                if i = splits then
                    workerSystem <! Info(startind, k, n)         
                elif i % 2L = 0L then
                    let endind = startind + subsize - 1L
                    workerSystem <! Info(startind, k, endind)
                    startind <- startind + subsize
                else
                    sref <! (startind, k, (startind + subsize - 1L))
                    startind <- startind + subsize
        | Done(complete) ->
            completed <- completed + 1L
            if completed = splits then
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()

        return! loop()
    }
    loop()


let boss = spawn system "boss" BossActor
// Input from Command Line
let N = fsi.CommandLineArgs.[1] |> int64
let K = fsi.CommandLineArgs.[2] |> int64
boss <! Input(N, K)
// Wait until all the actors has finished processing
system.WhenTerminated.Wait()
