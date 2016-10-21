{-# LANGUAGE TemplateHaskell, ScopedTypeVariables, CPP #-}

import Quelea.Shim
import Quelea.Types
import Quelea.ClientMonad
import Quelea.DBDriver
import BankAccountDefs
import Prelude hiding (catch)
import Quelea.Contract
import System.Process (ProcessHandle, runCommand, terminateProcess)
import System.Environment (getExecutablePath, getArgs)
import Control.Concurrent (ThreadId, myThreadId, forkIO, threadDelay, killThread)
import Quelea.NameService.Types
#ifdef LBB
import Quelea.NameService.LoadBalancingBroker
#else
import Quelea.NameService.SimpleBroker
#endif
import Quelea.Marshall
import Quelea.TH
import Database.Cassandra.CQL
import System.Posix.Process
import Control.Monad.Trans (liftIO)
import Data.Text (pack)
import Quelea.Types (summarize)
import Control.Monad (replicateM_, foldM, when, forever)
import Data.IORef
import Options.Applicative
import Data.Time
import Control.Concurrent.MVar
import System.Posix.Signals
import Control.Exception ( SomeException(..), AsyncException(..) , catch, handle, throw)
import System.Exit (exitSuccess)
import System.Random (randomIO)
import Data.Monoid
--------------------------------------------------------------------------------

fePort :: Int
fePort = 5558

bePort :: Int
bePort = 5559

tableName :: String
tableName = "BankAccount"

numOpsPerRound :: Num a => a
numOpsPerRound = 3

printEvery :: Int
printEvery = 1

--------------------------------------------------------------------------------

data Kind = Broker | Client | Server
          | Daemon | Drop | Create deriving (Read, Show)

data Args = Args {
  -- Kind of process
  kind :: String,
  -- Broker's address
  brokerAddr :: String,

  {- Daemon Options -}
  {- -------------- -}
  -- Optional rts arguments. Only relevant for Daemon.
  rtsArgs :: String,
  -- Terminate processes after time (microseconds). Only relevant for Daemon.
  terminateAfter :: String,

  {- Client Options -}
  {- -------------- -}
  -- Number of client rounds
  numRounds :: String,
  -- Number of concurrent client threads
  numThreads :: String,
  -- Delay between client requests in microseconds. Used to control throughput.
  delayReq :: String,
  -- Measure latency
  measureLatency :: Bool
}

args :: Parser Args
args = Args
  <$> strOption
      ( long "kind"
     <> metavar "KIND"
     <> help "Kind of process [Broker|Client|Server|Daemon|Drop|Create]" )
  <*> strOption
      ( long "brokerAddr"
     <> metavar "ADDR"
     <> help "Address of broker"
     <> value "localhost")
  <*> strOption
      ( long "rtsArgs"
     <> metavar "RTS_ARGS"
     <> help "RTS arguments passed to child processes. Only relevant for Daemon."
     <> value "")
  <*> strOption
      ( long "terminateAfter"
    <> metavar "SECS"
    <> help "Terminate child proceeses after time. Only relevant for Daemon"
    <> value "600")
  <*> strOption
      ( long "numRounds"
     <> metavar "NUM_ROUNDS"
     <> help "Number of client rounds"
     <> value "1000")
  <*> strOption
      ( long "numThreads"
     <> metavar "NUM_THREADS"
     <> help "Number of concurrent client threads"
     <> value "1")
  <*> strOption
      ( long "delayReq"
     <> metavar "MICROSECS"
     <> help "Delay between client requests"
     <> value "1000")
  <*> switch
      ( long "measureLatency"
     <> help "Measure operation latency" )
-------------------------------------------------------------------------------

keyspace :: Keyspace
keyspace = Keyspace $ pack "Quelea"

dtLib = mkDtLib [(Deposit, mkGenOp deposit summarize, Causal),
                 (Withdraw, mkGenOp withdraw summarize, Causal),
                 (GetBalance, mkGenOp getBalance summarize, Causal)]


run :: Args -> IO ()
run args = do
  let k = read $ kind args
  let broker = brokerAddr args
  let delay = read $ delayReq args
  someTime <- getCurrentTime
  let ns1 = mkNameService (Frontend $ "tcp://" ++ broker ++ ":" ++ show fePort)
                         (Backend  $ "tcp://" ++ broker ++ ":" ++ show bePort) "localhost" 5560

  let ns2 = mkNameService (Frontend $ "tcp://" ++ broker ++ ":" ++ show fePort)
                         (Backend  $ "tcp://" ++ broker ++ ":" ++ show bePort) "localhost" 5561 

  let ns3 = mkNameService (Frontend $ "tcp://" ++ broker ++ ":" ++ show fePort)
                         (Backend  $ "tcp://" ++ broker ++ ":" ++ show bePort) "localhost" 5562

  case k of
    Broker -> do
       startBroker (Frontend $ "tcp://*:" ++ show fePort)
                     (Backend $ "tcp://*:" ++ show bePort)
    Server -> do
       tid1 <- forkIO $ runShimNode dtLib [("127.0.0.1","9042")] keyspace ns1 0
       tid2 <- forkIO $ runShimNode dtLib [("127.0.0.2","9042")] keyspace ns2 1
       tid3 <- forkIO $ runShimNode dtLib [("127.0.0.3","9042")] keyspace ns3 2
       threadDelay 300000000
       putStrLn "Server is Shut Down"
    
    Client -> do
      let rounds = read $ numRounds args
      let threads = read $ numThreads args

      mv::(MVar NominalDiffTime)<- newEmptyMVar

      t1 <- getCurrentTime
      replicateM_ threads $ forkIO $ do
        avgLatency <- runSession ns1 $ foldM (clientCore args delay someTime) 0 [1 .. rounds]
        putMVar mv avgLatency
      replicateM_ threads $ forkIO $ do
        avgLatency <- runSession ns2 $ foldM (clientCore args delay someTime) 0 [1 .. rounds]
        putMVar mv avgLatency
      replicateM_ threads $ forkIO $ do
        avgLatency <- runSession ns3 $ foldM (clientCore args delay someTime) 0 [1 .. rounds]
        putMVar mv avgLatency
      totalLat <- foldM (\l _ -> takeMVar mv >>= \newL -> return $ l + newL) 0 [1..threads]
      t2 <- getCurrentTime
      putStrLn $ "Throughput (ops/s) = " ++ (show $ (fromIntegral $ numOpsPerRound * rounds * threads) / (diffUTCTime t2 t1))
      putStrLn $ "Latency (s) = " ++ (show $ (totalLat / fromIntegral threads))
    Create -> do
      pool <- newPool [("127.0.0.1","9042")] keyspace Nothing
      runCas pool $ createTable tableName
    Daemon -> do
      pool <- newPool [("127.0.0.1","9042"), ("127.0.0.2","9042"), ("127.0.0.3","9042")] keyspace Nothing
      runCas pool $ createTable tableName
      runCas pool $ createReservationTable
      progName <- getExecutablePath
      putStrLn "Driver : Starting broker"
      b <- runCommand $ progName ++ " +RTS " ++ (rtsArgs args)
                        ++ " -RTS --kind Broker --brokerAddr " ++ broker
      putStrLn "Driver : Starting server "
      s <- runCommand $ progName ++ " +RTS " ++ (rtsArgs args)
                        ++ " -RTS --kind Server --brokerAddr " ++ broker
  
      putStrLn "Driver : Starting client"
      c <- runCommand $ progName ++ " +RTS " ++ (rtsArgs args)
                        ++ " -RTS --kind Client --brokerAddr " ++ broker
                        ++ " --numThreads " ++ (numThreads args)
                        ++ " --numRounds " ++ (numRounds args)
                        ++ " --delayReq " ++ (delayReq args)
                        ++ if (measureLatency args) then " --measureLatency" else ""
      -- Install handler for Ctrl-C
      tid <- myThreadId
      installHandler keyboardSignal (Catch $ reportSignal pool [b,s,c] tid) Nothing
      -- Block
      let termWait = read $ terminateAfter args
      threadDelay (termWait * 1000000)
      -- Woken up..
      mapM_ terminateProcess [b,s,c]
      runCas pool $ dropTable tableName
      runCas pool $ dropReservationTable
    Drop -> do
      pool <- newPool [("localhost","9042")] keyspace Nothing
      runCas pool $ dropTable tableName
      runCas pool $ dropReservationTable

reportSignal :: Pool -> [ProcessHandle] -> ThreadId -> IO ()
reportSignal pool procList mainTid = do
  mapM_ terminateProcess procList
  runCas pool $ dropTable tableName
  runCas pool $ dropReservationTable
  killThread mainTid

clientCore :: Args -> Int -> UTCTime -- default arguments
           -> NominalDiffTime -> Int -> CSN NominalDiffTime
clientCore args delay someTime avgLat round = do
  --liftIO $ putStrLn  "--------------------------------"
  -- Generate key
  key <- liftIO $ (mkKey . (\i -> i `mod` (1::Int))) <$> randomIO
  -- Delay thread if required
  when (delay /= 0) $ liftIO $ threadDelay delay
  -- Perform the operations
  t1 <- getNow args someTime
  {-liftIO $ putStrLn  "invoking deposit"
  r::() <- invoke key Deposit (10::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r::() <- invoke key Deposit (9::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (1::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (5::Int) --
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (5::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (5::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r::() <- invoke key Deposit (9::Int) --
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (1::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (5::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r::() <- invoke key Deposit (9::Int) --
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (1::Int)
  liftIO $ threadDelay 100000
  liftIO $ putStrLn  "invoking deposit"
  r :: () <- invoke key Deposit (5::Int)-}
  
  r::() <- invoke key Deposit (1::Int)
  --liftIO $ putStrLn $ "Deposit : 1"
  r:: Bool <- invoke key Withdraw (1::Int)
  --liftIO $ putStrLn $ "Withdraw : 1 :: " ++ show r
  r :: Int <- invoke key GetBalance ()
  if r < 0 then do
    liftIO $ putStrLn $ "Balance : " ++ show r
  else do 
    return ()
  t2 <- getNow args someTime
  -- Calculate new latency
  let timeDiff = diffUTCTime t2 t1
  let newAvgLat = ((timeDiff / numOpsPerRound) + (avgLat * (fromIntegral $ round - 1))) / (fromIntegral round)
  -- Print info if required
  --when (round `mod` printEvery == 0) $ do
  --  liftIO . putStrLn $ "Round = " ++ show round ++ " result = " ++ show r
  --                      ++ if (measureLatency args)
  --                         then " latency = " ++ show newAvgLat
  --                          else ""
  return newAvgLat

getNow :: Args -> UTCTime -> CSN UTCTime
getNow args someTime =
  if (measureLatency args)
  then liftIO $ getCurrentTime
  else return someTime

main :: IO ()
main = execParser opts >>= run
  where
    opts = info (helper <*> args)
      ( fullDesc
     <> progDesc "Run the bank account benchmark"
     <> header "BankAccountBenchmark - A benchmark for bank account datatype on Quelea" )
