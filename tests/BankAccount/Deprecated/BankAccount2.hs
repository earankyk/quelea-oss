{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

import Quelea.Shim
import Quelea.ClientMonad
import Quelea.DBDriver
import BankAccountDefs
import Quelea.Contract
import System.Process (runCommand, terminateProcess)
import System.Environment (getExecutablePath, getArgs)
import Control.Concurrent (threadDelay)
import Quelea.NameService.SimpleBroker
import Quelea.Marshall
import Quelea.TH
import Database.Cassandra.CQL
import Control.Monad.Trans (liftIO)
import Data.Text (pack)
import Quelea.Client (mkKey, getUUID)
import Control.Applicative

fePort :: Int
fePort = 5558

bePort :: Int
bePort = 5559


data Kind = B | C | S | D deriving (Read, Show)

keyspace :: Keyspace
keyspace = Keyspace $ pack "Quelea"

dtLib = mkDtLib [(Deposit, mkGenOp deposit summarize, $(checkOp "Deposit" depositCtrt)),
                 (Withdraw, mkGenOp withdraw summarize, $(checkOp "Withdraw" withdrawCtrt)),
                 (GetBalance, mkGenOp getBalance summarize, $(checkOp "Withdraw" getBalanceCtrt))]

main :: IO ()
main = do
  (kindStr:tailStr) <- getArgs
  let k :: Kind = read kindStr
  case k of
    B -> startBroker (Frontend $ "tcp://*:" ++ show fePort)
                     (Backend $ "tcp://*:" ++ show bePort)
    S -> do
      let offStr:_ = tailStr
      let off::Int = read offStr
      print dtLib
      runShimNode dtLib [("localhost",show (9042+off))] keyspace
        (Backend $ "tcp://localhost:" ++ show bePort) (5560+off)
    C -> runSession (Frontend $ "tcp://localhost:" ++ show fePort) $ do
      let keyStr:_ = tailStr
      let key = mkKey . read $ keyStr
      liftIO $ putStrLn "Client : performing deposit"
      r::() <- invoke key Deposit (64::Int)

      liftIO $ putStrLn "Client : performing withdraw"
      r::Bool <- invoke key Withdraw (10::Int)
      liftIO . putStrLn $ show r

      liftIO $ putStrLn "Client : performing getBalance"
      r::Int <- invoke key GetBalance ()
      liftIO . putStrLn $ show r
    D -> do
      pool <- newPool [("localhost","9042")] keyspace Nothing
      runCas pool $ createTable "BankAccount"
      progName <- getExecutablePath
      putStrLn "Driver : Starting broker"
      b <- runCommand $ progName ++ " B"
      putStrLn "Driver : Starting server0"
      s0 <- runCommand $ progName ++ " S 0"
      -- putStrLn "Driver : Starting server1"
      -- s1 <- runCommand $ progName ++ " S 1"
      key <- liftIO $ newKey
      putStrLn "Driver : Starting client0"
      c0 <- runCommand $ progName ++ " C " ++ show (getUUID key)
      threadDelay 1000000
      putStrLn "Driver : Starting client1"
      c1 <- runCommand $ progName ++ " C " ++ show (getUUID key)
      threadDelay 5000000
      mapM_ terminateProcess [b,s0,c0,c1]
      runCas pool $ dropTable "BankAccount"
