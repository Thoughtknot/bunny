module Connections where
import Control.Concurrent (MVar, Chan, readMVar, newChan, readChan, forkIO, writeChan, newMVar)
import Lib (Incoming, Outgoing, addrFromString, MsgType (KillReq), NodeId)
import Network.Socket (PortNumber, HostName)
import qualified Client
import Data.Foldable (find)
import Data.Maybe (isNothing, isJust)
import Control.Monad (join, liftM)
import State

start :: MVar State -> Chan Incoming -> Chan Outgoing -> IO ()
start mstate inc out = do
    state <- readMVar mstate
    rout <- newChan
    run (return []) (current state) inc out rout

run :: IO [(HostName, PortNumber, MVar Bool)] -> NodeId -> Chan Incoming -> Chan Outgoing -> Chan Outgoing -> IO ()
run clients this inc iout rout = do
    (s, d, tp, payload) <- readChan iout
    if d == this then
        run clients this inc iout rout
    else do
        isCl <- isClient d clients
        let nClients =
                if isCl then do
                    clients
                else do
                    malive <- newMVar True
                    _ <- forkIO (Client.start this d malive rout inc)
                    append clients (fst d, snd d,malive)
        newClients <-
                if tp == KillReq then do
                    filter (\(a,b,c) -> (a,b) /= d) <$> nClients
                else
                    nClients
        
        let cls = filterOutDead newClients
        writeChan rout (s, d, tp, payload)
        run cls this inc iout rout

append :: IO [a] -> a -> IO [a]
append ls x = do
    l <- ls
    return (x : l)

filterOutDead :: [(HostName, PortNumber, MVar Bool)] -> IO [(HostName, PortNumber, MVar Bool)]
filterOutDead [] = return []
filterOutDead ((a,b,c):xs) = do
    alive <- readMVar c
    ls <- filterOutDead xs
    if alive then
        return ((a,b,c):ls)
    else
        return ls
    

isClient :: (HostName, PortNumber) -> IO [(HostName, PortNumber, MVar Bool)] -> IO Bool
isClient x ls = do
    isJust . find (\(a,b,c) -> (a,b) == x) <$> ls