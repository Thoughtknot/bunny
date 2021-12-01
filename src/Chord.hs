module Chord(Chord.start) where

import Lib
    (
      MsgType (FindSuccessorReq, GetPredecessorReq, NotifyReq, StabilizeReq, KillReq, FindSuccessorRsp, GetPredecessorRsp, NotifyRsp, GetValueReq, GetValueRsp, LookupValueReq, LookupValueRsp, SetValueReq, PutValueReq, SetValueRsp, PutValueRsp, SubscribeReq, SendReq, CreateTopicReq),
      getId, addrFromString, addrToString, Payload, Outgoing, Incoming, NodeId, Request, Response, between, between', SubMessage )
import Control.Concurrent ( forkIO, readChan, Chan, writeChan, newMVar, MVar, readMVar, swapMVar, dupChan )
import Data.Maybe (fromMaybe, fromJust, isNothing, isJust)
import Control.Monad (when, forM_)
import Network.Socket
import GHC.OldList (find)
import Control.Concurrent.Chan (newChan)
import GHC.Conc.IO (threadDelay)
import Debug.Trace
import qualified Server
import qualified Stabilize
import qualified Connections
import Data.Map
import qualified Data.Map as Map
import Data.List.Split.Internals (splitOn)
import qualified APIServer
import Data.List.Split (splitOn)
import State
import qualified MessageHandler
import MessageHandler (MessageOp)

start :: State -> Chan Request -> Chan Response -> IO()
start state req rsp = do
    incoming <- newChan
    restoutgoing <- newChan
    clientoutgoing <- newChan
    apichanoutgoing <- newChan
    messagechan <- newChan
    let (a,b) = current state
    let lis = (a, listener state)
    mstate <- newMVar state
    _ <- forkIO $ Stabilize.start mstate incoming clientoutgoing
    _ <- forkIO $ Connections.start mstate incoming clientoutgoing
    _ <- forkIO $ Server.start (current state) incoming
    _ <- forkIO $ APIServer.start lis incoming apichanoutgoing
    _ <- forkIO $ serveClient mstate req rsp incoming restoutgoing
    _ <- forkIO $ MessageHandler.start (messageQueues state) apichanoutgoing messagechan
    run mstate incoming clientoutgoing restoutgoing messagechan

serveClient :: MVar State -> Chan Request -> Chan Response -> Chan Incoming -> Chan Outgoing -> IO ()
serveClient mstate req rsp inc out = do
    (k, tp, v) <- readChan req
    state <- readMVar mstate
    (s, d, tp, v) <- case tp of
        GetValueReq -> do
            writeChan inc (current state, successor state, GetValueReq, k)
            readChan out
        SetValueReq -> do
            print $ "Writing " ++ show (current state, successor state, SetValueReq, k ++ "///" ++ v)
            writeChan inc (current state, successor state, SetValueReq, k ++ "///" ++ v)
            readChan out
        _ -> error $ "not implemented " ++ show tp
    writeChan rsp (k, tp, v)
    serveClient mstate req rsp inc out

wrap :: IO a -> IO ()
wrap k = do
    _ <- k
    return ()

run :: MVar State -> Chan Incoming -> Chan Outgoing -> Chan Outgoing -> Chan MessageOp ->IO ()
run mstate incoming coutgoing routgoing apioutgoing = do
    (s, d, tp, payload) <- readChan incoming
    handleRequest mstate (s, d, tp, payload) coutgoing routgoing apioutgoing
    run mstate incoming coutgoing routgoing apioutgoing

handleRequest :: MVar State -> (NodeId, NodeId, MsgType, Payload) -> Chan Outgoing -> Chan Outgoing -> Chan MessageOp -> IO ()
handleRequest state (s, d, tp, payload) cout rout apiout
    | tp == GetPredecessorReq = getPredecessor state s cout
    | tp == NotifyReq = notify state payload cout
    | tp == NotifyRsp = stabilize state payload cout
    | tp == GetPredecessorRsp = stabilize state payload cout
    | tp == KillReq = removeConnection state s
    | tp == LookupValueReq = findValue state (s, d, payload) cout
    | tp == LookupValueRsp = rspValue state (s, d, payload) cout rout
    | tp == SetValueReq = setValue state payload cout rout
    | tp == PutValueReq = putValue state (s, d, payload) cout
    | tp == PutValueRsp = putValueRsp state (s, d, payload) cout rout
    | tp == GetValueReq = getValue state payload cout rout
    | tp == SubscribeReq = writeChan apiout (SubscribeReq , payload)
    | tp == SendReq = writeChan apiout (SendReq, payload)
    | tp == CreateTopicReq = writeChan apiout (CreateTopicReq, payload)
    | otherwise = error $ "Unknown request " ++ show tp
{-
createTopic :: Payload -> MVar State -> Chan (String, Chan SubMessage) -> IO ()
createTopic p mstate chan = do 
    state <- readMVar mstate
    MessageHandler.create p (messageQueues state)

sendMsg :: Payload -> MVar State -> IO ()
sendMsg payload mstate = do
    state <- readMVar mstate
    MessageHandler.write payload (messageQueues state)

subscribe :: Payload -> MVar State -> Chan (String, Chan SubMessage) -> IO ()
subscribe p mstate chan = do
    state <- readMVar mstate
    MessageHandler.subscribe p (messageQueues state) chan
-}

findValue :: MVar State -> (NodeId, NodeId, Payload) -> Chan Outgoing -> IO ()
findValue mstate (s, d, payload) cout = do
    state <- readMVar mstate
    -- Ugly shit
    let key = read payload
    let pred = getId $ case predecessor state of
            Nothing -> current state
            Just x -> x
    let curr = getId $ current state
    if between' key pred curr then do
        print $ "Found value " ++ show (s,d,payload)
        case store state !? payload of
            Just p -> writeChan cout (current state, s, LookupValueRsp, p)
            Nothing -> writeChan cout (current state, s, LookupValueRsp, "Nothing")
    else
        writeChan cout (s, successor state, LookupValueReq, payload)

rspValue :: MVar State -> (NodeId, NodeId, Payload) -> Chan Outgoing -> Chan Outgoing -> IO ()
rspValue mstate (s, d, payload) cout rout = do
    state <- readMVar mstate
    -- Ugly shit
    if d == current state then do
        writeChan rout (current state, current state, GetValueRsp, payload)
    else
        writeChan cout (s, successor state, LookupValueRsp, payload)

getValue :: MVar State -> Payload -> Chan Outgoing -> Chan Outgoing -> IO ()
getValue mstate payload cout rout = do
    state <- readMVar mstate
    -- Ugly shit
    let key = read payload
    let pred = getId $ case predecessor state of
            Nothing -> current state
            Just x -> x
    let curr = getId $ current state
    if between' key pred curr then do
        case store state !? payload of
            Just p -> writeChan rout (current state, current state, GetValueRsp, p)
            Nothing -> writeChan rout (current state, current state, GetValueRsp, "Nothing")
    else do
        writeChan cout (current state, successor state, LookupValueReq, payload)

putValueRsp :: MVar State -> (NodeId, NodeId, Payload) -> Chan Outgoing -> Chan Outgoing -> IO ()
putValueRsp mstate (s, d, payload) cout rout = do
    state <- readMVar mstate
    if d == current state then do
        writeChan rout (current state, current state, SetValueRsp, payload)
    else
        writeChan cout (s, successor state, PutValueRsp, payload)

putValue :: MVar State -> (NodeId, NodeId, Payload) -> Chan Outgoing -> IO ()
putValue mstate (s, d, payload) cout = do
    state <- readMVar mstate
    -- Ugly shit
    let [k,v] = splitOn "///" payload
    let key = read k
    let pred = getId $ case predecessor state of
            Nothing -> current state
            Just x -> x
    let curr = getId $ current state
    if between' key pred curr then do
        print $ "Set value " ++ show payload
        swapMVar mstate (state {store=Map.insert k v (store state)})
        writeChan cout (current state, s, PutValueRsp, v)
    else
        writeChan cout (s, successor state, PutValueReq, payload)

setValue :: MVar State -> Payload -> Chan Outgoing -> Chan Outgoing -> IO ()
setValue mstate payload cout rout = do
    state <- readMVar mstate
    -- Ugly shit
    let [k,v] = splitOn "///" payload
    let key = read k
    let pred = getId $ case predecessor state of
            Nothing -> current state
            Just x -> x
    let curr = getId $ current state
    if between' key pred curr then do
        print $ "Set value " ++ show payload
        swapMVar mstate (state {store=Map.insert k v (store state)})
        writeChan rout (current state, current state, SetValueRsp, v)
    else do
        writeChan cout (current state, successor state, PutValueReq, payload)

removeConnection :: MVar State -> NodeId -> IO ()
removeConnection mstate node = do
    state <- readMVar mstate
    let newSuccessor =
            if successor state == node then
                current state
            else
                successor state
    let newPredecessor =
            case predecessor state of
                Nothing -> Nothing
                Just x -> if x == node then Nothing else Just x
    swapMVar mstate (state {successor=newSuccessor, predecessor=newPredecessor})
    return ()


stabilize :: MVar State -> Payload -> Chan Outgoing -> IO ()
stabilize mstate payload outgoing = do
    state <- readMVar mstate
    let not = (current state, successor state, NotifyReq, addrToString (current state))
    let pred = if payload == "" then Nothing else Just $ addrFromString payload
    case pred of
        Nothing -> writeChan outgoing not
        Just x ->
            if x == successor state then
                -- My successor has no predecessor
                writeChan outgoing not
            else if x == current state then
                -- I am the predecessor of the successor 
                return ()
            else do
                if between x (current state) (successor state) then do
                    swapMVar mstate (state {successor=x})
                    writeChan outgoing (current state, x, GetPredecessorReq, "")
                else
                    writeChan outgoing not
    return ()

notify :: MVar State -> Payload -> Chan Outgoing -> IO ()
notify mstate payload outgoing = do
    state <- readMVar mstate
    let newPred = addrFromString payload
    case predecessor state of
        Nothing -> do
            swapMVar mstate (state {predecessor=Just newPred})
            writeChan outgoing (current state, newPred, NotifyRsp, payload)
        Just paddr -> do
            if between newPred paddr (current state) then do
                swapMVar mstate (state {predecessor=Just newPred})
                writeChan outgoing (current state, newPred, NotifyRsp, payload)
            else
                writeChan outgoing (current state, newPred, NotifyRsp, addrToString paddr)

getPredecessor :: MVar State -> NodeId -> Chan Outgoing -> IO ()
getPredecessor mstate sender outgoing = do
    state <- readMVar mstate
    case predecessor state of
        Nothing -> writeChan outgoing (current state, sender, GetPredecessorRsp, "")
        Just addr -> writeChan outgoing (current state, sender, GetPredecessorRsp, addrToString addr)