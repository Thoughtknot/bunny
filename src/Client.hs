{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
module Client (start) where

import Network.Socket
import System.IO
import Control.Exception
import Control.Concurrent
import Control.Monad (when)
import Control.Monad.Fix (fix)
import System.Environment (getArgs)

import Lib ( Incoming, Outgoing, MsgType (FindSuccessorReq, KillReq, FindSuccessorRsp, NotifyRsp, GetPredecessorRsp), addrToString, NodeId, addrFromString )
import Debug.Trace (trace)
import Data.Either (isLeft, fromRight)
import Data.List.Split (splitOn)

start :: NodeId -> NodeId -> MVar Bool -> Chan Outgoing -> Chan Incoming -> IO ()
start name (addr, port) malive outgoing incoming = do
    newOutgoing <- dupChan outgoing
    print $ "Starting client connection to " ++ show (addr, port)
    addrInfo:_ <- getAddrInfo Nothing (Just addr) (Just $ show port)
    sock <- openSocket addrInfo
    either <- try $ connect sock (addrAddress addrInfo) :: IO (Either SomeException ())
    case either of
        Left ex -> do
            print $ "Error connecting to " ++ show (addr, port) ++ ", retrying"
        Right conn -> do
            print $ "Succeeded in connecting to " ++ show (addr, port)
            hdl <- socketToHandle sock ReadWriteMode
            hSetBuffering hdl NoBuffering
            hPutStrLn hdl (addrToString name)
            handle (\(SomeException x) -> trace (show x) return ()) $ runConn (addr, port) hdl newOutgoing incoming
    swapMVar malive False
    writeChan incoming ((addr,port), (addr,port), KillReq, "")
    return ()

runConn :: NodeId -> Handle -> Chan Outgoing  -> Chan Incoming -> IO ()
runConn name hdl outgoing incoming = do
    (sender, dest, tp, payload) <- readChan outgoing
    if name == dest then
        if tp == KillReq then do
            print $ show name ++ " killed by request"
            return ()
        else do
            hPutStrLn hdl (addrToString sender ++ ":" ++ addrToString dest ++ ":" ++ show tp ++ ":" ++ payload)
            runConn name hdl outgoing incoming
    else do
        runConn name hdl outgoing incoming