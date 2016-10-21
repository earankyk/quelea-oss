{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

import Quelea.Shim
import Quelea.ClientMonad
import Quelea.Marshall
import Quelea.NameService.Types
import Quelea.NameService.SimpleBroker
import Quelea.TH

import System.Process (runCommand, terminateProcess)
import System.Environment (getExecutablePath, getArgs)
import Database.Cassandra.CQL hiding (Write, Read)
import Control.Monad.Trans (liftIO)
import Data.Text (pack)
import Quelea.Types (summarize)
import Control.Monad (replicateM_)
import Control.Concurrent (threadDelay)
import Data.Int (Int64)

import LWWRegisterDefs

fePort :: Int
fePort = 5558

bePort :: Int
bePort = 5559


data Kind = B | C | S | D | Drop deriving (Read, Show)

keyspace :: Keyspace
keyspace = Keyspace $ pack "LWWRegister"

dtLib = mkDtLib [(HAWrite, mkGenOp writeReg summarize, $(checkOp HAWrite haWriteCtrt)),
                 (CAUWrite, mkGenOp writeReg summarize, $(checkOp CAUWrite haWriteCtrt)),
                 (STWrite, mkGenOp writeReg summarize, $(checkOp STWrite haWriteCtrt)),
                 (HARead, mkGenOp readReg summarize, $(checkOp HARead haReadCtrt)),
                 (CAURead, mkGenOp readReg summarize, $(checkOp CAURead haReadCtrt)),
                 (STRead, mkGenOp readReg summarize, $(checkOp STRead haReadCtrt))]

haRead :: Key -> CSN Int
haRead k = invoke k HARead ()

main :: IO ()
main = do
  (kindStr:broker:restArgs) <- getArgs
  let k :: Kind = read kindStr
  let ns = mkNameService (Frontend $ "tcp://" ++ broker ++ ":" ++ show fePort)
                         (Backend  $ "tcp://" ++ broker ++ ":" ++ show bePort) "localhost" 5560
  case k of
    B -> startBroker (Frontend $ "tcp://*:" ++ show fePort)
                     (Backend $ "tcp://*:" ++ show bePort)

    S -> do
      runShimNode dtLib [("localhost","9042")] keyspace ns

    C -> do
      runSession ns $ do
        return ()

    D -> do
      pool <- newPool [("localhost","9042")] keyspace Nothing
      runCas pool $ createTables
      progName <- getExecutablePath
      putStrLn "Driver : Starting broker"
      b <- runCommand $ progName ++ " B"
      putStrLn "Driver : Starting server"
      s <- runCommand $ progName ++ " S"
      putStrLn "Driver : Starting client"
      c <- runCommand $ progName ++ " C"
      threadDelay 5000000
      mapM_ terminateProcess [b,s,c]
      runCas pool $ dropTables

    Drop -> do
      pool <- newPool [("localhost", "9042")] keyspace Nothing
      runCas pool $ dropTables
