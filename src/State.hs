module State where
import Lib
import Network.Socket
import Control.Concurrent
import MessageHandler
import Data.Map

data State = State {
    current :: NodeId,
    listener :: PortNumber,
    predecessor :: Maybe NodeId,
    successor :: NodeId,
    store :: Map String String,
    messageQueues :: MVar MessageQueues
}