module Server (start) where
import Network.Socket
import System.IO
import Control.Exception
import Control.Concurrent
import Control.Monad (when)
import Control.Monad.Fix (fix)
import System.Environment (getArgs)
import Data.List.Split (splitOn)

import Lib ( Incoming, MsgType (FindSuccessorReq, NotifyReq, GetPredecessorReq, GetPredecessorRsp, KillReq, NotifyRsp), addrToString, addrFromString, NodeId )
import System.Environment.Blank (getArgs)
import Debug.Trace (trace)
import GHC.Exception.Type (Exception)

start :: (HostName, PortNumber) -> Chan Incoming -> IO ()
start (addr, port) ireqchan = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    addr:_ <- getAddrInfo Nothing (Just addr) (Just $ show port)
    bind sock $ addrAddress addr
    listen sock 2
    print $ "Starting service on port " ++ show port
    acceptConn sock ireqchan

acceptConn :: Socket -> Chan Incoming -> IO ()
acceptConn sock ireqchan = do
    conn <- accept sock
    hdl <- socketToHandle (fst conn) ReadWriteMode
    hSetBuffering hdl NoBuffering
    name <- hGetLine hdl
    print $ "Accepted incoming connection " ++ name
    _ <- forkIO $ handle (\(SomeException x) -> handleFailure (addrFromString name) ireqchan x) $ handleIncoming hdl ireqchan
    acceptConn sock ireqchan

handleFailure :: Exception e => NodeId -> Chan Incoming -> e -> IO ()
handleFailure node chan err = do
    print $ "Error: " ++ show err ++ ", killing " ++ show node
    writeChan chan (node, node, KillReq, "")

handleIncoming :: Handle -> Chan Incoming -> IO ()
handleIncoming hdl ireq = do
    line <- hGetLine hdl 
    handleRequest ireq line
    handleIncoming hdl ireq

handleRequest :: Chan Incoming -> String -> IO ()
handleRequest reqchan req = writeChan reqchan (sender, dest, read tp, payload)
    where
        (s:d:tp:payload:xs) = splitOn ":" req
        sender = addrFromString s
        dest = addrFromString d