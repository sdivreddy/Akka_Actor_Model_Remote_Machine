open System.Threading
#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

type Information = 
    | Info of (int64*int64*int64)
    | Done of (string)

//configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = 192.168.0.88
                }
            }
        }")

let system = ActorSystem.Create("RemoteSquares", configuration)

//Remote Child Actors - Performs the processing of subproblems assigned by the boss using sliding window mechanism 
// and returns the responses to the client machine after its completion.
type RemoteChildActor() =
    inherit Actor()
    override x.OnReceive message =
        let x : Information = downcast message
        let sref = select "akka.tcp://Squares@192.168.0.29:8777/user/Printer" system
        match x with
        | Info(startind,k,endind) -> 
            let mutable sum = 0L
            for i in startind .. startind+k-1L do
                sum <- sum + (i*i)
                
            let value = sum |> double |> sqrt |> int64
            if sum = (value * value) then
                sref <! startind

            for i in startind+1L .. endind do
                sum <- sum + ((i+k-1L)*(i+k-1L)) - ((i-1L)*(i-1L))
                let value = sum |> double |> sqrt |> int64
                if sum = (value * value) then
                    sref <! i
            
            sref <! "Done"

//Remote Boss Actor - Receives subproblem from the client machine and assigns it to the child actors in the same machine.
//It maintains an actor pool based on the cores' count and allocates subproblems in the Round-Robin approach. 
let RemoteBossActor = 
    spawn system "RemoteBossActor"
    <| fun mailbox ->
        let actcount = System.Environment.ProcessorCount |> int64
        let tot = actcount*1L
        let remoteChildActorsPool = 
                [1L .. tot]
                |> List.map(fun id -> system.ActorOf(Props(typedefof<RemoteChildActor>)))

        let childenum = [|for rp in remoteChildActorsPool -> rp|]
        let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(childenum)))
        
        let rec loop() =
            actor {
                let! (message:obj) = mailbox.Receive()
                let (startindex,k,endindex) : Tuple<int64,int64,int64> = downcast message
                workerSystem <! Info(startindex,k,endindex)
                return! loop()
            }
        printf "Remote Boss Started \n" 
        loop()

System.Console.ReadLine()      
