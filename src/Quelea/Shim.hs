{-# LANGUAGE ScopedTypeVariables, EmptyDataDecls,
    TemplateHaskell, DataKinds, OverloadedStrings,
    DoAndIfThenElse, ForeignFunctionInterface, 
    OverloadedStrings, DeriveGeneric #-}

module Quelea.Shim (
 runShimNode,
 runShimNodeWithOpts,
 mkDtLib
) where

import Quelea.Types
import Quelea.Consts
import Quelea.NameService.Types
import Quelea.Marshall
import Quelea.DBDriver
import Quelea.ShimLayer.Cache
import Quelea.ShimLayer.GC
import Quelea.ShimLayer.Types
import Quelea.Contract.Language
import Quelea.Client(mkKey)

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Data.Serialize
import Control.Applicative ((<$>))
import Control.Monad (forever, replicateM, when)
import Data.ByteString hiding (map, pack, putStrLn)
import Data.Either (rights)
import Data.Map (Map)
import Data.IORef
import System.IO.Unsafe (unsafePerformIO)
import qualified Data.Map as M
import System.ZMQ4.Monadic
import qualified System.ZMQ4 as ZMQ
import Data.Maybe (fromJust)
import Data.Word
import Control.Lens
import Control.Monad.State.Lazy
import System.Posix.Process
import Database.Cassandra.CQL
import Data.UUID
import Data.Int (Int64)
import qualified Data.Set as S
import Data.Text hiding (map)
import Debug.Trace
import Control.Concurrent (forkIO, myThreadId, threadDelay)
import Data.Tuple.Select
import Foreign.C
import Data.ByteString.Char8 (pack, packCString, unpack)
import Foreign.Marshal.Array
import Foreign.Ptr
import Web.Scotty
import Data.Aeson (FromJSON, ToJSON)
import GHC.Generics
import Data.Serialize
import System.Random (randomIO)

makeLenses ''Addr
makeLenses ''DatatypeLibrary
makeLenses ''OperationPayload
makeLenses ''CacheManager

#define DEBUG

debugPrint :: String -> IO ()
#ifdef DEBUG
debugPrint s = do
  tid <- myThreadId
  putStrLn $ "[" ++ (show tid) ++ "] " ++ s
#else
debugPrint _ = return ()
#endif

data RESTEffect = RESTEffect { effect :: String } deriving (Show, Generic)
instance ToJSON RESTEffect
instance FromJSON RESTEffect

data RESTAppend = RESTAppend { objType :: String, key :: Int, effectString :: String } deriving (Generic, Show)
instance ToJSON RESTAppend
instance FromJSON RESTAppend

runShimNodeWithOpts :: OperationClass a
                    => GCSetting
                    -> Int -- fetch update interval
                    -> DatatypeLibrary a
                    -> [Server] -> Keyspace -- Cassandra connection info
                    -> NameService
                    -> ShimID
                    -> SeqNo
                    -> [ObjType]
                    -> IO ()
runShimNodeWithOpts gcSetting fetchUpdateInterval dtLib serverList keyspace ns shimId kBound objTypes = do
  pidshim <- getProcessID 
  {- Connection to the Cassandra deployment -}
  pool <- newPool serverList keyspace Nothing
  {- Spawn cache manager -}
  cache <- initCacheManager pool fetchUpdateInterval shimId kBound objTypes
  let sqn = 1
  sid <- randomIO
  forkIO $ scotty (3000 + shimId) $ do
    Web.Scotty.get "/read/:objType/:key" $ do
      key <- param "key" 
      let val = read key :: Int
      effects <- liftIO $ getEffects cache "BankAccount" (mkKey val)
      json effects
    Web.Scotty.post "/append" $ do
      appendEffect <- jsonData :: ActionM RESTAppend
      --liftIO $ putStrLn $ (objType appendEffect) ++ " " ++ show (key appendEffect) ++ " " ++ (effectString appendEffect)
      liftIO $ writeEffect cache (objType appendEffect) (mkKey (key appendEffect)) (Addr (SessID sid) sqn) (Data.ByteString.Char8.pack (effectString appendEffect)) S.empty ONE Nothing
      let sqn = sqn + 1 
      return ()
  {- Spawn a pool of workers -}
  replicateM cNUM_WORKERS (forkIO $ worker dtLib pool cache gcSetting shimId)
  --putStrLn $ "Shim #" ++ show shimId ++ " initialized"  ++ " --- Accessing servers@" ++ show serverList
  case gcSetting of
    No_GC -> getServerJoin ns
    GC_Mem_Only -> getServerJoin ns
    otherwise -> do
      {- Join the broker to serve clients -}
      forkIO $ getServerJoin ns
      {- Start gcWorker -}
      gcWorker dtLib cache
  where 
    getEffects cache objType key = do
      putStrLn $ "Searching effects for key : " ++ show key
      (effects, _) <- getContext cache objType key
      return $ map ((\a -> RESTEffect a) . Data.ByteString.Char8.unpack) effects

runShimNode :: OperationClass a
            => DatatypeLibrary a
            -> [Server] -> Keyspace -- Cassandra connection info
            -> NameService
            -> ShimID
            -> SeqNo
            -> [ObjType]
            -> IO ()
runShimNode = runShimNodeWithOpts GC_Full cCACHE_THREAD_DELAY

-- Function worker does a write
worker :: OperationClass a => DatatypeLibrary a -> Pool -> CacheManager -> GCSetting -> ShimID -> IO ()
worker dtLib pool cache gcSetting shimId = do
  ctxt <- ZMQ.context
  sock <- ZMQ.socket ctxt ZMQ.Rep
  pid <- getProcessID
  -- debugPrint "worker: connecting..."
  -- Connecting to process id of shim?
  ZMQ.connect sock $ "ipc:///tmp/quelea/" ++ show pid
  --debugPrint ("worker: connected for Shim #" ++ show shimId)
  {- loop forver servicing clients -}
  forever $ do
    binReq <- ZMQ.receive sock
    txns <- includedTxns cache
    case decodeOperationPayload binReq of
      ReqOper req -> do
        --Prelude.putStr $ (show (req^.sqnReq))  ++ " => " 
        {- Fetch the operation from the datatype library using the object type and
        - operation name. -}
        let (op,av) =
              case dtLib ^. avMap ^.at (req^.objTypeReq, req^.opReq) of
                Nothing -> error $ "Not found in DatatypeLibrary:" ++ (show (req^.objTypeReq, req^.opReq))
                Just x -> x
        -- debugPrint $ "worker: before " ++ show (req^.objTypeReq, req^.opReq, av)
        (result, ctxtSize) <- case av of
          Eventual -> doOp op cache req ONE
          Causal -> processCausalOp req op cache
          Strong -> processStrongOp req op cache pool
        ZMQ.send sock [] $ encode result
        -- debugPrint $ "worker: after " ++ show (req^.objTypeReq, req^.opReq)
        -- Maybe perform summarization
        let gcFun =
              case dtLib ^. sumMap ^.at (req^.objTypeReq) of
                Nothing -> error "Worker(2)"
                Just x -> x
        case gcSetting of
          No_GC -> return ()
          otherwise -> maybeGCCache cache (req^.objTypeReq) (req^.keyReq) ctxtSize gcFun
        return ()
      ReqTxnCommit txid deps -> do
        -- debugPrint $ "Committing transaction " ++ show txid
        when (S.size deps > 0) $ runCas pool $ insertTxn txid deps
        ZMQ.send sock [] $ encode ResCommit
      ReqSnapshot objs -> do
        fetchUpdates cache ALL $ S.toList objs
        snapshot <- snapshotCache cache
        let filteredSnapshot = M.foldlWithKey (\m k v ->
              if S.member k objs then M.insert k v m else m) M.empty snapshot
        ZMQ.send sock [] $ encode $ ResSnapshot filteredSnapshot
  where
    processCausalOp req op cache =
      -- Check whether this is the first effect in the session <= previous
      -- sequence number is 0.
      if req^.sqnReq == 0
      then doOp op cache req ONE
      else do
        let ot = req^.objTypeReq
        let k = req^.keyReq
        -- Check whether the current cache includes the previous effect
        res <- doesCacheInclude cache ot k (req^.sidReq) (req^.sqnReq)
        if res
        then doOp op cache req ONE
        else do
          -- Read DB, and check cache for previous effect
          fetchUpdates cache ONE [(ot,k)]
          res <- doesCacheInclude cache ot k (req^.sidReq) (req^.sqnReq)
          if res
          then doOp op cache req ONE
          else do
            -- Wait till next cache refresh and repeat the process again
            waitForCacheRefresh cache ot k
            processCausalOp req op cache
    processStrongOp req op cache pool = do
      let (ot, k, sid) = (req^.objTypeReq, req^.keyReq, req^.sidReq)
      -- Get Lock
      getLock ot k sid pool
      -- debugPrint $ "processStrongOp: obtained lock"
      -- Read latest values at the key - under ALL
      fetchUpdates cache ALL [(ot,k)]
      -- Perform the op
      res <- doOp op cache req ALL
      -- Release Lock
      releaseLock ot k sid pool
      return res

doOp :: OperationClass a => GenOpFun -> CacheManager -> OperationPayload a -> Consistency -> IO (Response, Int)
doOp op cache request const = do
  let (OperationPayload objType key operName arg sessid seqno mbtxid getDeps) = request
  --seqnoList <- getCollumnByShimId cache
  --shimId <- readMVar $ cache^.shimId
  --if (seqnoList !! shimId) < (Prelude.minimum seqnoList) + cK_BOUND then do
    -- Build the context
  (ctxt, deps) <- buildContext objType key mbtxid
  -- Perform the operation on this context
  -- debugPrint $ "doOp: length of context = " ++ show (Prelude.length ctxt)
  let (res, effM) = op ctxt arg
  -- Add current location to the ones for which updates will be fetched
  addHotLocation cache objType key
  let resDeps = if getDeps then deps else S.empty
  result <- case effM of
    Nothing -> --do
      --updateInMemVC cache
      return $ ResOper seqno res Nothing Nothing resDeps
    Just eff -> do
      -- Write effect writes to DB, and potentially to cache
      writeEffect cache objType key (Addr sessid (seqno+1)) eff deps const $ sel1 <$> mbtxid
      case mbtxid of
        Nothing -> return $ ResOper (seqno + 1) res Nothing Nothing resDeps
        Just (_,MAV_TxnPl _ _) -> do
          txns <- getInclTxnsAt cache objType key
          return $ ResOper (seqno + 1) res (Just eff) (Just txns) resDeps
        otherwise -> return $ ResOper (seqno + 1) res (Just eff) Nothing resDeps
  -- return response
  return (result, Prelude.length ctxt)
  --else do
  --  waitForCacheRefresh cache objType key
  --  doOp op cache request const
  where
    buildContext ot k Nothing = getContext cache ot k
    buildContext ot k (Just (_, RC_TxnPl l)) = do
      (ctxtVanilla, depsVanilla) <- buildContext ot k Nothing
      let (el, as) = S.foldl (\(el,as) (addr, eff) ->
                      (eff:el, S.insert addr as)) ([],S.empty) l
      return (el ++ ctxtVanilla, S.union as depsVanilla)
    buildContext ot k (Just (txid, MAV_TxnPl l txndeps)) = do
      res <- doesCacheIncludeTxns cache txndeps
      if res then buildContext ot k (Just (txid,RC_TxnPl l))
      else do
        fetchTxns cache txndeps
        buildContext ot k (Just (txid, MAV_TxnPl l txndeps))
    buildContext ot k (Just (_,RR_TxnPl effSet)) = return $
      S.foldl (\(el,as) (addr, eff) -> (eff:el, S.insert addr as))
              ([], S.empty) effSet

{-foo :: ObjType -> Key -> State ([Quelea.ShimLayer.Types.Effect], CacheManager)
foo ot k = runState (do 
                      cm <- Control.Monad.State.Lazy.get
                      (effects, _) <- getContext cm ot k
                      Control.Monad.State.Lazy.put cm
                      return effects) initialState-}

{-foreign export ccall readEffects :: CString -> CString -> IO (Ptr CString)--[CString]--[Quelea.ShimLayer.Types.Effect]
readEffects ot k = do
  cm <- readMVar test
  key <- packCString k
  objectType <- peekCString ot
  (effects, _) <- getContext cm objectType (Key key)
  effectsList <- mapM (newCString . Data.ByteString.Char8.unpack) effects
  newArray effectsList-}

mkDtLib :: OperationClass a => [(a, (GenOpFun, GenSumFun), Availability)] -> DatatypeLibrary a
mkDtLib l =
  let (m1, m2) = Prelude.foldl core (M.empty, M.empty) l
  in DatatypeLibrary m1 m2
  where
    core (m1, m2) (op, (fun1, fun2), av) =
      (M.insert (getObjType op, op) (fun1, av) m1,
       M.insert (getObjType op) fun2 m2)