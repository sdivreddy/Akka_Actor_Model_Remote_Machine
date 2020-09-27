open System
open Akka.FSharp
open Akka.Actor
open Akka.Configuration
                // debug : {
                //     receive : on
                //     autoreceive : on
                //     lifecycle : on
                //     event-stream : on
                //     unhandled : on
                // }

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
                port = 8778
                hostname = 192.168.0.88
            }
        }")

type Information = 
    | Info of (int64*int64*int64)
    | Done of (string)
    | Input of (int64*int64)
let mutable completed = 0L

let remoteSystemAddress = "akka.tcp://RemoteSquares@192.168.0.29:8777"
let system = System.create "Squares" (configuration)

let printerActor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let boss = system.ActorSelection("akka.tcp://Squares@192.168.0.88:8778/user/delegator")
        let! (message:obj) = mailbox.Receive()
        if (message :? string) then
            boss <! Done("Done Remote")
        else
            let (index:int64) = downcast message
            printfn "%d" index      
        return! loop()
    }
    loop()

let remoteDeploy systemPath =
    let address =
        match ActorPath.TryParseAddress systemPath with
        | false, _ -> failwith "Actor Path addr failed to be parsed"
        | true, a -> a
    Deploy(RemoteScope(address))

let printerRef = spawn system "Printer" printerActor

let remoter idnum =
    spawne system (sprintf "remote_%d" idnum)
        <@
            let rec add (i:int64) (ed:int64) (sm:int64) = 
                let mutable sum = sm
                for j in i..ed do
                    let newval = sum + (i*i)
                    sum <- newval
                sum

            let squareroot (i:int64) = 
                let value = i |> float |> sqrt
                let vali = value |> int64
                vali

            let rec calcWindow (j:int64) (ed:int64) (sm:int64) (k:int64) (send:ActorSelection) = 
                let mutable smm = sm
                for i in j+1L .. ed do
                    let newval = smm + ((i+k-1L)*(i+k-1L)) - ((i-1L)*(i-1L))
                    smm <- newval
                    let value = squareroot smm
                    if smm = (value * value) then
                        send <! i  
                        printfn "loacl if= %d" i 
                smm       

            fun mailbox ->
            let rec loop(): Cont<string, unit> =
                actor {
                    let! msg = mailbox.Receive()
                    let sender = mailbox.ActorSelection("akka.tcp://Squares@192.168.0.88:8778/user/Printer")
                    match msg with
                    | m ->
                        let given : string = m
                        let result = Seq.toList (given.Split ',')
                        let startind = result.[0] |> int64
                        let k = result.[1] |> int64
                        let endind = result.[2] |> int64
                        let sum = add startind (startind+k-1L) (0L)
                        let value = squareroot sum
                        let prod = value * value
                        if sum = prod then
                           sender <! startind
                           printfn "loacl if= %d" startind
                        let finalsum = calcWindow (startind+1L) endind sum k sender  
                        sender <! "Done"                      
                        ignore

                    | _ -> logErrorf mailbox "Received unexpected message: %A" msg
                        
                    return! loop()
                }
            loop()
        @> [ SpawnOption.Deploy(remoteDeploy remoteSystemAddress) ]

let ChildActor (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! (message : Information) = mailbox.Receive()
        let x : Information = message
        match x with
        | Info(startind,k,endind) -> 
            let mutable sum = 0L
            for i in startind .. startind+k-1L do
                sum <- sum + (i*i)
                
            let value = sum |> double |> sqrt |> int64
            if sum = (value * value) then
                printerRef <! startind
                printfn " if= %d" startind

            for i in startind+1L .. endind do
                sum <- sum + ((i+k-1L)*(i+k-1L)) - ((i-1L)*(i-1L))
                let value = sum |> double |> sqrt |> int64
                if sum = (value * value) then
                    printfn " if= %d" i
                    printerRef <! i

            printerRef <! "Done"
        return! loop()
    }
    loop()


//Takes input from the command line arguments and spawns the Delegator actor which creates and maintains a pool
//of actors for remote system and local system deployment.
[<EntryPoint>]
let main args =
    let N = args.[1] |> int64
    let K = args.[2] |> int64

    let delegator (mailbox:Actor<_>) = 
        let actorcount = System.Environment.ProcessorCount |> int64
        let totalActors = actorcount*1L
        let localPool = 
            [1L .. totalActors]
            |> List.map(fun id -> spawn system (sprintf "Local_%d" id) ChildActor)

        let localenum = [|for lp in localPool -> lp|]
        let localSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(localenum)))
        let remotePool = 
            [1L .. totalActors]
            |> List.map(fun id -> (remoter id))

        let remoteenum = [|for rp in remotePool -> rp|]
        let remoteSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(remoteenum)))

        let rec delloop () = 
            actor {
                let! mail = mailbox.Receive()
                let splits = totalActors*2L
                match mail with 
                | Input(n,k) -> 
                    let subsize = n/splits
                    let mutable startind = 1L

                    for i in [1L..splits] do
                        if i = splits then
                            localSystem <! Info(startind, k, n)        
                        elif i%2L = 0L then
                            let endind = startind + subsize - 1L
                            localSystem <! Info(startind, k, endind)
                            startind <- startind + subsize
                        else
                            let remoteInput = string startind + "," + string k + "," + string (startind + subsize - 1L)
                            remoteSystem <! remoteInput
                | Done(complete) -> 
                    completed <- completed + 1L
                    if completed = splits then
                        mailbox.Context.System.Terminate() |> ignore

                return! delloop()
            }
        delloop()

    let del = spawn system "delegator" delegator
    del <! Input(N,K)

    system.WhenTerminated.Wait()
    0