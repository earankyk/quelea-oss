{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

import Quelea.Shim
import Quelea.ClientMonad
import Quelea.DBDriver
import BankAccountDefs
import Quelea.Contract
import System.Process (runCommand, terminateProcess)
import System.Environment (getExecutablePath, getArgs)
import Control.Concurrent (threadDelay)
import Quelea.NameService.Types
import Quelea.NameService.SimpleBroker
import Quelea.Marshall
import Quelea.TH
import Database.Cassandra.CQL
import Control.Monad.Trans (liftIO)
import Data.Text (pack)
import Quelea.Types (summarize)
import Control.Monad (replicateM_, when, forever)
import Data.IORef

fePort :: Int
fePort = 5558

bePort :: Int
bePort = 5559


data Kind = B | C | S | D deriving (Read, Show)

keyspace :: Keyspace
keyspace = Keyspace $ pack "Quelea"

dtLib = mkDtLib [(Deposit, mkGenOp deposit summarize, $(checkOp Deposit depositCtrt)),
                 (Withdraw, mkGenOp withdraw summarize, $(checkOp Withdraw withdrawCtrt)),
                 (GetBalance, mkGenOp getBalance summarize, $(checkOp GetBalance getBalanceCtrt))]

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

    C -> runSession ns $ do
      -- liftIO $ threadDelay 100000
      key <- liftIO $ newKey
      liftIO $ putStrLn "Client : performing deposit"
      r::() <- invoke key Deposit (64::Int)

      liftIO $ putStrLn "Client : performing withdraw"
      r::Bool <- invoke key Withdraw (10::Int)
      liftIO . putStrLn $ show r

      liftIO $ putStrLn "Client : performing getBalance"
      r::Int <- invoke key GetBalance ()
      liftIO . putStrLn $ show r

      replicateM_ 16 $ do
        r::() <- invoke key Deposit (1::Int)
        r :: Int <- invoke key GetBalance ()
        liftIO . putStrLn $ show r
    D -> do
      pool <- newPool [("localhost","9042")] keyspace Nothing
      runCas pool $ createTable "BankAccount"
      progName <- getExecutablePath
      putStrLn "Driver : Starting broker"
      b <- runCommand $ progName ++ " B"
      putStrLn "Driver : Starting server"
      s <- runCommand $ progName ++ " S"
      putStrLn "Driver : Starting client"
      c <- runCommand $ progName ++ " C"
      threadDelay 5000000
      mapM_ terminateProcess [b,s,c]
      runCas pool $ dropTable "BankAccount"
