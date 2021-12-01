module Stabilize(Stabilize.start) where
import Control.Concurrent (MVar, readMVar, Chan, writeChan, readChan, swapMVar)
import Lib (Incoming, Outgoing, addrToString, MsgType (GetPredecessorReq, NotifyReq, GetPredecessorRsp), addrFromString, getId)
import GHC.Conc.IO (threadDelay)
import Control.Monad (when)
import Network.Socket (getAddrInfo)
import State

start :: MVar State -> Chan Incoming -> Chan Outgoing -> IO ()
start mstate incoming clientout = do
    state <- readMVar mstate
    --print (current state)
    stabilize state incoming clientout
    threadDelay 10000000
    start mstate incoming clientout

stabilize :: State  -> Chan Incoming -> Chan Outgoing -> IO ()
stabilize state inchan outchan
    | successor state == current state = writeChan inchan (current state, current state, GetPredecessorRsp, pred)
    | otherwise = writeChan outchan (current state, successor state, GetPredecessorReq, payload)
    where
        payload = addrToString (current state)
        pred = maybe "" addrToString (predecessor state)