{-# LANGUAGE TemplateHaskell, ScopedTypeVariables, CPP #-}

import Quelea.Shim
import Quelea.ClientMonad
import Quelea.DBDriver
import Quelea.Contract
import Control.Concurrent (ThreadId, myThreadId, forkIO, threadDelay, killThread)
import Quelea.NameService.Types
import Quelea.Types (summarize)
import Quelea.Marshall
import Quelea.TH
#ifdef LBB
import Quelea.NameService.LoadBalancingBroker
#else
import Quelea.NameService.SimpleBroker
#endif


import Data.Monoid
import Data.Time
import Prelude hiding (catch)
import Control.Monad (replicateM_, foldM, when, forever, foldM_, replicateM)
import Control.Monad.Trans (liftIO)
import Control.Concurrent.MVar
import Control.Exception ( SomeException(..), AsyncException(..) , catch, handle, throw)
import Data.IORef
import Data.Text (pack)
import Data.Time
import Database.Cassandra.CQL
import Options.Applicative
import System.Environment (getExecutablePath, getArgs)
import System.Exit (exitSuccess)
import System.Posix.Signals
import System.Process (ProcessHandle, runCommand, terminateProcess)
import System.Random
import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import System.IO (hFlush, stdout)

import FusionTicketDefs
import FusionTicketCtrts
import FusionTicketTxns

-- #define DEBUG

debugPrint :: String -> CSN ()
#ifdef DEBUG
debugPrint s = liftIO $ do
  tid <- myThreadId
  putStrLn $ "[" ++ (show tid) ++ "] " ++ s
#else
debugPrint _ = return ()
#endif

--------------------------------------------------------------------------------

fePort :: Int
fePort = 5558

bePort :: Int
bePort = 5559

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
  rtsArgs :: String}

--------------------------------------------------------------------------------
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

keyspace :: Keyspace
keyspace = Keyspace $ pack "Quelea"

[
  createEventCtrtA,
  stopSalesCtrtA,
  restartSalesCtrtA,
  returnSeatsCtrtA,
  sellCtrtA,
  deleteEventCtrtA,
  showEventCtrtA,
  createPlacemapCtrtA,
  updateEventIdCtrtA,
  addSeatsToPlacemapCtrtA,
  showPlacemapCtrtA,
  deletePlacemapCtrtA,
  createDiscountCtrtA,
  deleteDiscountCtrtA,
  showDiscountCtrtA,
  createSeatCtrtA,
  freeCtrtA,
  deleteSeatCtrtA,
  reserveCtrtA,
  showSeatCtrtA,
  updateDiscountInSeatCtrtA,
  updateEventIdAndPriceInSeatCtrtA,
  addCartCtrtA,
  removeCartCtrtA,
  createOrderCtrtA,
  cancelOrderCtrtA,
  updateOrderCtrtA,
  getOrderSummaryCtrtA,
  addOrdersToCartCtrtA,
  removeOrdersFromCartCtrtA,
  getCartSummaryCtrtA ] =
    $(do
        t1 <- runIO getCurrentTime
        a1 <- checkOp CreateEvent createEventCtrt
        a2 <- checkOp StopSales stopSalesCtrt
        a3 <- checkOp RestartSales restartSalesCtrt
        a4 <- checkOp Return returnSeatsCtrt
        a5 <- checkOp Sell sellCtrt
        a6 <- checkOp DeleteEvent deleteEventCtrt
        a7 <- checkOp ShowEvent showEventCtrt
        b1 <- checkOp CreatePlacemap createPlacemapCtrt
        b2 <- checkOp UpdateEventId updateEventIdCtrt
        b3 <- checkOp AddSeatsToPlacemap addSeatsToPlacemapCtrt
        b4 <- checkOp ShowPlacemap showPlacemapCtrt
        b5 <- checkOp DeletePlacemap deletePlacemapCtrt
        c1 <- checkOp CreateDiscount createDiscountCtrt
        c2 <- checkOp DeleteDiscount deleteDiscountCtrt
        c3 <- checkOp ShowDiscount showDiscountCtrt
        d1 <- checkOp CreateSeat createSeatCtrt
        d2 <- checkOp Free freeCtrt
        d3 <- checkOp Delete deleteSeatCtrt
        d4 <- checkOp Reserve reserveCtrt
        d5 <- checkOp ShowSeat showSeatCtrt
        d6 <- checkOp UpdateDiscountInSeat updateDiscountInSeatCtrt
        d7 <- checkOp UpdateEventIdAndPriceInSeat updateEventIdAndPriceInSeatCtrt
        e1 <- checkOp AddCart addCartCtrt
        e2 <- checkOp RemoveCart removeCartCtrt
        f1 <- checkOp CreateOrder createOrderCtrt
        f2 <- checkOp CancelOrder cancelOrderCtrt
        f3 <- checkOp UpdateOrder updateOrderCtrt
        f4 <- checkOp GetOrderSummary getOrderSummaryCtrt
        g1 <- checkOp AddOrdersToCart addOrdersToCartCtrt
        g2 <- checkOp RemoveOrdersFromCart removeOrdersFromCartCtrt
        g3 <- checkOp GetCartSummary getCartSummaryCtrt
        le <- return $ (ListE::[Exp] -> Exp)
                [a1,a2,a3,a4,a5,a6,a7,b1,b2,b4,b4,b5,c1,c2,c3,d1,d2,d3,d4,d5,d6,d7,e1,e2,f1,f2,f3,f4,g1,g2,g3]
        t2 <- runIO getCurrentTime
        _ <- runIO $ putStrLn $ "----------------------------------------------------------"
        _ <- runIO $ putStrLn $ "Classification of operation contracts completed in "++
                  (show $ diffUTCTime t2 t1)++"."
        _ <- runIO $ putStrLn $ "----------------------------------------------------------"
        _ <- runIO $ hFlush stdout
        return le)

dtLib = mkDtLib
            [(CreateEvent, mkGenOp createEvent summarize, createEventCtrtA),
             (StopSales, mkGenOp stopSales summarize, stopSalesCtrtA),
             (RestartSales, mkGenOp restartSales summarize, restartSalesCtrtA),
             (Return, mkGenOp returnSeats summarize, returnSeatsCtrtA),
             (Sell, mkGenOp sell summarize, sellCtrtA),
             (DeleteEvent, mkGenOp deleteEvent summarize, deleteEventCtrtA),
             (ShowEvent, mkGenOp showEvent summarize, showEventCtrtA),

             (CreatePlacemap, mkGenOp createPlacemap summarize, createPlacemapCtrtA),
             (UpdateEventId, mkGenOp updateEventId summarize, updateEventIdCtrtA),
             (DeletePlacemap, mkGenOp deletePlacemap summarize, deletePlacemapCtrtA),
             (AddSeatsToPlacemap, mkGenOp addSeatsToPlacemap summarize, addSeatsToPlacemapCtrtA),
             (ShowPlacemap, mkGenOp showPlacemap summarize, showPlacemapCtrtA),

             (CreateDiscount, mkGenOp createDiscount summarize, createDiscountCtrtA),
             (DeleteDiscount, mkGenOp deleteDiscount summarize, deleteDiscountCtrtA),
             (ShowDiscount, mkGenOp showDiscount summarize, showDiscountCtrtA),

             (CreateSeat, mkGenOp createSeat summarize, createSeatCtrtA),
             (Reserve, mkGenOp reserve summarize, reserveCtrtA),
             (Free, mkGenOp free summarize, freeCtrtA),
             (Delete, mkGenOp deleteSeat summarize, deleteSeatCtrtA),
             (ShowSeat, mkGenOp showSeat summarize, showSeatCtrtA),
             (UpdateDiscountInSeat, mkGenOp updateDiscountInSeat summarize, updateDiscountInSeatCtrtA),
             (UpdateEventIdAndPriceInSeat, mkGenOp updateEventIdAndPriceInSeat summarize, updateEventIdAndPriceInSeatCtrtA),

             (CreateOrder, mkGenOp createOrder summarize, createOrderCtrtA),
             (CancelOrder, mkGenOp cancelOrder summarize, cancelOrderCtrtA),
             (UpdateOrder, mkGenOp updateOrder summarize, updateOrderCtrtA),
             (GetOrderSummary, mkGenOp getOrderSummary summarize, getOrderSummaryCtrtA),

             (AddOrdersToCart, mkGenOp addOrdersToCart summarize, addOrdersToCartCtrtA),
             (RemoveOrdersFromCart, mkGenOp removeOrdersFromCart summarize, removeOrdersFromCartCtrtA),
             (GetCartSummary, mkGenOp getCartSummary summarize, getCartSummaryCtrtA),

             (AddCart, mkGenOp addCart summarize, addCartCtrtA),
             (RemoveCart, mkGenOp removeCart summarize, removeCartCtrtA)]

run :: Args -> IO ()
run args = do
  let k = read $ kind args
  let broker = brokerAddr args
  let ns = mkNameService (Frontend $ "tcp://" ++ broker ++ ":" ++ show fePort)
                         (Backend  $ "tcp://" ++ broker ++ ":" ++ show bePort) "localhost" 5560
  case k of
    Broker -> startBroker (Frontend $ "tcp://*:" ++ show fePort)
                     (Backend $ "tcp://*:" ++ show bePort)
    Server -> do
      runShimNode dtLib [("localhost","9042")] keyspace ns 0
    Client -> do
      runSession ns $ do 
        (pmid, seatids) <- newPlacemap 10000
        --eventid <- newEvent pmid "sample-name" "sample.com" (UTCTime (fromGregorian 2016 12 16) 0) 10000 10 2
        key <- liftIO $ randomIO
        let cartID = CartID key
        r::() <- invoke (mkKey key) AddCart ()
        return ()
        {-let seats = take 10 seatids
        orderid <- newOrder cartID eventid seats 
        r::() <- invoke (mkKey key) AddOrdersToCart (orderid)
        val <- checkOut cartID-}
    Create -> do
      pool <- newPool [("localhost","9042")] keyspace Nothing
      runCas pool $ createTables
    Daemon -> do
      pool <- newPool [("localhost","9042")] keyspace Nothing
      runCas pool $ createTables
      progName <- getExecutablePath
      b <- runCommand $ progName ++ " +RTS " ++ (rtsArgs args)
                        ++ " -RTS --kind Broker --brokerAddr " ++ broker
      putStrLn "Driver : Starting server"
      s <- runCommand $ progName ++ " +RTS " ++ (rtsArgs args)
                        ++ " -RTS --kind Server --brokerAddr " ++ broker
      putStrLn "Driver : Starting client"
      c <- runCommand $ progName ++ " +RTS " ++ (rtsArgs args)
                        ++ " -RTS --kind Client --brokerAddr " ++ broker
      -- Install handler for Ctrl-C
      tid <- myThreadId
      installHandler keyboardSignal (Catch $ reportSignal pool [b,s,c] tid) Nothing
      threadDelay 50000000
      -- Woken up..
      mapM_ terminateProcess [b,s,c]
      runCas pool $ dropTables
    Drop -> do
      pool <- newPool [("localhost","9042")] keyspace Nothing
      runCas pool $ dropTables

main :: IO ()
main = execParser opts >>= run
  where
    opts = info (helper <*> args)
      ( fullDesc
     <> progDesc "Run the Fusion ticket benchmark"
     <> header "Fusion Ticket Benchmark" )

reportSignal :: Pool -> [ProcessHandle] -> ThreadId -> IO ()
reportSignal pool procList mainTid = do
  mapM_ terminateProcess procList
  runCas pool $ dropTables
  killThread mainTid