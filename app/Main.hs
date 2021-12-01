module Main where

import Network.Socket
import qualified Network.Socket as HostAddress
import Control.Concurrent
import Control.Monad.Fix (fix)
import System.Environment (getArgs)
import Server ( start )
import Chord ( start )
import Lib (NodeId, getId)
import State(State(State), messageQueues, store, predecessor,successor, current, listener)
import Rest ( start )
import qualified MessageHandler
import qualified Data.Map as Map
import qualified Data.Foldable as Int

main :: IO ()
main = do
  args <- getArgs
  let port = read (head args) :: PortNumber
  let s = getSuccessor port (tail args)
  req <- newChan
  rsp <- newChan
  let curr = ("127.0.0.1", port)
  let perc = 100 * fromIntegral (getId curr) / fromIntegral (maxBound :: Int)
  print $ "Current id: " ++ show (getId curr) ++ ", %" ++ show perc
  _ <- forkIO $ Rest.start curr req rsp
  qs <- MessageHandler.init
  Chord.start State{messageQueues=qs, current=curr, predecessor=Nothing, listener=port - 2000, successor=s, store=Map.empty} req rsp

getSuccessor :: PortNumber -> [String] -> NodeId
getSuccessor port [] = ("127.0.0.1", port)
getSuccessor port (x:xs) = ("127.0.0.1", read x)