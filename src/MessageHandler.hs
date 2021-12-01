module MessageHandler where
import Control.Concurrent
import Lib ( SubMessage, Payload, Incoming, MsgType (CreateTopicReq, CreateTopicRsp, SendReq, SubscribeReq) )
import Data.List.Split ( splitOn )
import Control.Monad (forM_)
import Data.HashMap.Strict ( HashMap, (!?), (!))
import qualified Data.HashMap.Strict as HashMap
import Data.Vector.Mutable (MVector)
import qualified Data.Vector.Mutable as MVector
import GHC.Base ( RealWorld )
import Data.Maybe (fromJust)

type MessageOp = (MsgType, Payload)

newtype MessageQueues = MessageQueues {
    queues :: HashMap String (Chan SubMessage, MVar Int, MVector RealWorld SubMessage)
}

init :: IO (MVar MessageQueues)
init = do
    newMVar (MessageQueues HashMap.empty)

start :: MVar MessageQueues -> Chan (String, Chan SubMessage)-> Chan MessageOp -> IO ()
start mstate inc op = do
    (tp, v) <- readChan op
    case tp of
        CreateTopicReq -> create v mstate
        SendReq -> write v mstate
        SubscribeReq -> subscribe v mstate inc
        _ -> error $ "Unknown tp " ++ show (tp, v)
    start mstate inc op

create :: String -> MVar MessageQueues -> IO ()
create session mstate = do
    print $ "Creating queue " ++ session
    state <- readMVar mstate
    newchan <- newChan
    newindex <- newMVar 0
    q <- MVector.new 1000000
    swapMVar mstate (state{queues=HashMap.insert session (newchan, newindex, q) (queues state)})
    return ()

write :: Payload -> MVar MessageQueues -> IO ()
write payload mstate = do
    state <- readMVar mstate
    let [p, v] = splitOn "," payload
    let qs = queues state ! p
    let (newchan, i, nq) = qs
    idx <- takeMVar i
    putMVar i (idx + 1)
    --print $ "Sending message " ++ v ++ " to " ++ p ++ " at idx " ++ show idx
    MVector.write nq idx v
    writeChan newchan v
    return ()

subscribe :: Payload -> MVar MessageQueues -> Chan (String, Chan SubMessage) -> IO ()
subscribe p mstate chan = do
    state <- readMVar mstate
    let [name, key] = splitOn "///" p
    let (newchan, idx, q) = queues state ! key
    i <- readMVar idx
    nc <- dupChan newchan
    -- Write all queued up messages
    forM_ [0..(i-1)] $ \i -> do
        v <- MVector.read q i
        writeChan nc v
    writeChan chan (name, nc)
    return ()