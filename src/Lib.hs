{-# LANGUAGE DeriveGeneric #-}
module Lib (
    SubMessage,
    between, between', getId, addrToString, addrFromString,
    Request, Response, Key, Value, SenderId, DestinationId, NodeId, Payload, 
    Incoming, Outgoing,
    MsgType(SendReq, SubscribeReq, SendRsp, SubscribeRsp, CreateTopicReq, CreateTopicRsp,
    GetValueReq, LookupValueReq, PutValueReq, PutValueRsp, GetValueRsp, LookupValueRsp, SetValueReq, SetValueRsp, 
    StabilizeReq, FindSuccessorReq, GetPredecessorReq, NotifyReq, KillReq, FindSuccessorRsp, GetPredecessorRsp, NotifyRsp)) where
import Network.Socket (PortNumber, HostName)
import Data.List.Split (splitOn)
import Data.Map (Map)
import Data.Hashable
import GHC.Generics (Generic)
import Data.Aeson (ToJSON, FromJSON)
import Debug.Trace (trace)
import Control.Concurrent

type SubMessage = String

data MsgType = SendRsp | SubscribeRsp | CreateTopicReq | CreateTopicRsp | SendReq 
 | SubscribeReq | LookupValueReq | LookupValueRsp | PutValueReq | PutValueRsp | GetValueReq | GetValueRsp | SetValueReq | SetValueRsp | FindSuccessorReq | GetPredecessorReq | NotifyReq | StabilizeReq | KillReq
 | FindSuccessorRsp | GetPredecessorRsp | NotifyRsp deriving (Eq, Show, Read, Generic)

type Payload = String
type NodeId = (HostName, PortNumber)
type SenderId = NodeId
type DestinationId = NodeId
type Msg = (SenderId, DestinationId, MsgType, Payload)
type Incoming = Msg
type Outgoing = Msg

type Key = String
type Value = String

type Request = (Key, MsgType, Value)
type Response = (Key, MsgType, Value)

instance ToJSON MsgType
instance FromJSON MsgType

getId :: NodeId -> Int
getId (a, p) = abs $ hash (a ++ show p)

addrToString :: NodeId -> String
addrToString (addr,port) = addr ++ "//" ++ show port

addrFromString :: String -> NodeId
addrFromString str = (addr, port)
    where
        [a, b] = splitOn "//" str
        addr = a
        port = read b

between' :: Int -> Int -> Int -> Bool
between' key from to
    | from == to = True
    | from < to = key > from && key <= to
    | from > to = (key > from && key >= to) || (key < from && key <= to)
    | otherwise = trace (show ("Invalid between call", key, from, to)) False

between :: (HostName, PortNumber) -> (HostName, PortNumber) -> (HostName, PortNumber) -> Bool
between k f t
    | from == to = True
    | from < to = key > from && key <= to
    | from > to = (key > from && key >= to) || (key < from && key <= to)
    | otherwise = trace (show ("Invalid between call", key, from, to)) False
    where
        from = getId f
        to = getId t
        key = getId k