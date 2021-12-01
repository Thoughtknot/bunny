{-# LANGUAGE OverloadedStrings, DeriveGeneric #-}
module Rest where

import Web.Scotty
import Lib (Incoming, NodeId, Request, Response, MsgType (GetValueReq, SetValueReq), Key, Value)
import Control.Concurrent ( Chan, writeChan, readChan )
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.ByteString (ByteString)
import Data.Aeson (ToJSON(toJSON), FromJSON)
import Network.Socket (PortNumber)
import Data.ByteString.Char8 (unpack)
import Data.Aeson.Types (ToJSON(toJSON))
import GHC.Generics (Generic)

data Result = Result {
    key :: Key,
    tp :: MsgType,
    value :: Value 
} deriving (Eq, Show, Generic)

instance ToJSON Result
instance FromJSON Result

start :: NodeId -> Chan Request -> Chan Response -> IO ()
start id req rsp = do
    let port = read $ show (snd id - 1000) 
    scotty port $ do
        get "/store/:key" $ do
            key <- param "key"
            (k,t,v) <- liftIO $ getValue id key req rsp
            let r = Result{key=k, tp=t, value=v}
            json r
        put "/store/:key" $ do
            key <- param "key"
            rd <- bodyReader
            (k,t,v) <- liftIO $ putValue id key rd req rsp
            let r = Result{key=k, tp=t, value=v}
            json r

putValue :: NodeId -> String -> IO ByteString -> Chan Request -> Chan Response -> IO (Key, MsgType, Value)
putValue n key bs req rsp = do
        val <- bs
        writeChan req (key, SetValueReq, unpack val)
        (k, tp, v) <- readChan rsp
        return (k, tp, v)

getValue :: NodeId -> String -> Chan Request -> Chan Response -> IO (Key, MsgType, Value)
getValue n key req rsp = do
        writeChan req (key, GetValueReq, "")
        (k, tp, v) <- readChan rsp
        return (k, tp, v)
