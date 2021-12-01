module APIServer where
import Network.Socket
import Control.Concurrent
import Lib
import Control.Exception (SomeException(SomeException), Exception, handle)
import System.IO
import Data.List.Split (splitOn)
import Control.Monad (when)

start :: (HostName, PortNumber) -> Chan Incoming -> Chan (String, Chan SubMessage) -> IO ()
start (addr, port) ireqchan subchan = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    addr:_ <- getAddrInfo Nothing (Just addr) (Just $ show port)
    bind sock $ addrAddress addr
    listen sock 2
    print $ "Starting API service on port " ++ show port
    acceptConn sock ireqchan subchan

acceptConn :: Socket -> Chan Incoming -> Chan (String, Chan SubMessage) -> IO ()
acceptConn sock ireqchan subchan = do
    conn <- accept sock
    hdl <- socketToHandle (fst conn) ReadWriteMode
    hSetBuffering hdl NoBuffering
    hPutStrLn hdl "Client Id:"
    name <- hGetLine hdl
    print $ "Accepted incoming connection: " ++ name
    newsubchan <- dupChan subchan
    _ <- forkIO $ handle (\(SomeException x) -> handleFailure ("incoming " ++ name) x) $ handleIncoming name hdl ireqchan
    _ <- forkIO $ handle (\(SomeException x) -> handleFailure ("outgoing " ++ name) x) $ handleSubs name hdl newsubchan
    acceptConn sock ireqchan subchan

handleSubs :: String -> Handle -> Chan (String, Chan SubMessage) -> IO ()
handleSubs name hdl chan = do
    joinedChan <- newChan
    _ <- forkIO $ handle (\(SomeException x) -> handleFailure ("outgoing " ++ name ++ " sub") x) $ handleClientSub name hdl joinedChan
    handleInitSubs name joinedChan chan

handleInitSubs :: String -> Chan SubMessage -> Chan (String, Chan SubMessage) -> IO ()
handleInitSubs name jchan inchan = do
    (s, ch) <- readChan inchan
    when (name == s) $ do
        --print $ name ++ " got subscription for " ++ s
        _ <- forkIO $ handle (\(SomeException x) -> handleFailure ("outgoing " ++ name ++ " sub " ++ s) x) $ handleSub s jchan ch
        return ()
    handleInitSubs name jchan inchan

handleSub :: String -> Chan SubMessage -> Chan SubMessage -> IO ()
handleSub name jchan inchan = do
    readChan inchan >>= writeChan jchan
    handleSub name jchan inchan

handleClientSub :: String -> Handle -> Chan SubMessage -> IO ()
handleClientSub name hdl ch = do
    r <- readChan ch
    --print $ "Got message for " ++ name
    hPutStrLn hdl r
    handleClientSub name hdl ch

handleIncoming :: String -> Handle -> Chan Incoming -> IO ()
handleIncoming name hdl inc = do
    l1 <- hGetLine hdl
    let line = [z | z<-l1, z /= '\r']
    let (op, payload) = (take 3 line, drop 4 line)
    let tmpNode = (name :: HostName, 0 :: PortNumber)
    case op of
        "sub" -> writeChan inc (tmpNode, tmpNode, SubscribeReq, name ++ "///" ++ payload)
        "msg" -> writeChan inc (tmpNode, tmpNode, SendReq, payload)
        "cre" -> writeChan inc (tmpNode, tmpNode, CreateTopicReq, payload)
        _ -> do
            hClose hdl
            error $ "Unknown message: " ++ line
    handleIncoming name hdl inc

handleFailure :: Exception e => String -> e -> IO ()
handleFailure node err = print $ "Error: " ++ show err ++ ", killing " ++ node