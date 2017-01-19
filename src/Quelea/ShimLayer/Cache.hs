{-# LANGUAGE ScopedTypeVariables, EmptyDataDecls, TemplateHaskell, DataKinds, OverloadedStrings, DoAndIfThenElse, BangPatterns  #-}

module Quelea.ShimLayer.Cache (
  CacheManager,

  initCacheManager,
  getContext,
  addHotLocation,
  writeEffect,
  doesCacheInclude,
  waitForCacheRefresh,
  fetchUpdates,
  includedTxns,
  doesCacheIncludeTxns,
  fetchTxns,
  snapshotCache,
  getInclTxnsAt,
) where

import Quelea.Consts
import Control.Concurrent
import Control.Concurrent.MVar
import Data.ByteString hiding (map, pack, putStrLn, foldl, length, filter)
import Control.Lens
import qualified Data.Map as M
import qualified Data.Set as S
import System.Posix.Process (getProcessID)
import Data.Map.Lens
import Control.Monad (forever, when, replicateM, foldM)
import Data.Maybe (fromJust)
import Database.Cassandra.CQL
import Control.Monad.State
import System.IO
import Control.Applicative ((<$>))
import Data.Tuple.Select
import Data.Time

import Quelea.Types
import Quelea.ShimLayer.Types
import Quelea.DBDriver
import Quelea.ShimLayer.UpdateFetcher

makeLenses ''CacheManager

#define DEBUG

debugPrint :: String -> IO ()
#ifdef DEBUG
debugPrint s = do
  tid <- myThreadId
  pid <- getProcessID
  putStrLn $ "[" ++ (show pid) ++ "," ++ (show tid) ++ "] " ++ s
  hFlush stdout
#else
debugPrint _ = return ()
#endif


initCacheManager :: Pool -> Int -> ShimID -> SeqNo -> [ObjType] -> IO CacheManager
initCacheManager pool fetchUpdateInterval shimid kBound objTypes = do
  cache <- newMVar M.empty
  cursor <- newMVar M.empty
  nearestDeps <- newMVar M.empty
  lastGCAddr <- newMVar M.empty
  lastGCTime <- newMVar M.empty
  seenTxns <- newMVar (S.empty, M.empty)
  hwm <- newMVar M.empty
  drc <- newMVar M.empty
  hotLocs <- newMVar S.empty
  sem <- newEmptyMVar
  sem1 <- newEmptyMVar
  sem2 <- newEmptyMVar
  boundMVar <- newMVar ()
  blockedList <- newMVar []
  opCount <- newMVar 0
  kBoundedOpsCount <- newMVar 0
  vc <- initializeVC
  inMemVClock <- newMVar vc
  shimId <- newMVar shimid
  kBound <- newMVar kBound
  objectTypes <- newMVar $ S.fromList objTypes
  let cm = CacheManager cache cursor nearestDeps lastGCAddr lastGCTime
                        seenTxns hwm drc hotLocs sem sem1 sem2 blockedList pool 
                        shimId opCount kBoundedOpsCount inMemVClock boundMVar 
                        kBound objectTypes
  forkIO $ cacheMgrCore cm
  forkIO $ vcCronJob cm
  forkIO $ printReservationStats cm
  forkIO $ signalGenerator sem fetchUpdateInterval
  forkIO $ signalGenerator sem1 10
  forkIO $ signalGenerator sem2 10000000
  return $ cm
  where
    signalGenerator semMVar fetchUpdateInterval = forever $ do
      isEmpty <- isEmptyMVar semMVar
      if isEmpty
      then tryPutMVar semMVar ()
      else return True
      threadDelay fetchUpdateInterval
    initializeVC = do
      let seqList = Prelude.take cNUM_REPLICAS (repeat 0)
      let shimList = [0..(cNUM_REPLICAS - 1)]
      return $ foldl (\m i -> M.insert i seqList m) M.empty shimList 

vcCronJob :: CacheManager -> IO()
vcCronJob cm = forever $ do
  takeMVar $ cm^.semMVar1
  inMemClock <- takeMVar $ cm^.inMemVClock
  shimId <- readMVar $ cm^.shimId
  let x = M.lookup shimId inMemClock 
  case x of
    Just rseqnos -> do
      runCas (cm^.pool) $ cqlReservationUpdate (shimId, rseqnos)
    otherwise -> return ()
  reservationRows <- runCas (cm^.pool) $ cqlReservationRead
  putMVar (cm^.inMemVClock) $ buildVClockFromTable reservationRows inMemClock
  where
    buildVClockFromTable :: [ReservationRow] -> InMemoryVC -> InMemoryVC
    buildVClockFromTable reservationRows inMemClock =
      foldl (\e (shimid, rseqnos) -> 
                M.insert shimid rseqnos e) inMemClock reservationRows

printReservationStats :: CacheManager -> IO ()
printReservationStats cm = forever $ do
  takeMVar $ cm^.semMVar2
  shimId <- readMVar $ cm^.shimId
  no_ops <- readMVar $ cm^.opCount
  putStrLn $ "************************        No. of ops on Shim #" ++ show shimId ++ " --> " ++ show no_ops ++ "        ************************"
  inMemClock <- readMVar (cm^.inMemVClock)
  let collList = getCollumnByShimId shimId inMemClock
  putStrLn $ "************************        Max Diff on Shim # "++ show shimId ++ " : " ++ show ((Prelude.maximum collList) - (Prelude.minimum collList)) ++ "        ************************"
  kBoundedCount <- readMVar (cm^.kBoundedOpsCount)
  putStrLn $ "************************        No. of k-bounded ops on Shim #" ++ show shimId ++ " --> " ++ show kBoundedCount ++ "        ************************"

getInclTxnsAt :: CacheManager -> ObjType -> Key -> IO (S.Set TxnID)
getInclTxnsAt cm ot k = do
  inclTxns <- readMVar $ cm^.includedTxnsMVar
  case M.lookup (ot,k) $ sel2 inclTxns of
    Nothing -> return S.empty
    Just s -> return s

addHotLocation :: CacheManager -> ObjType -> Key -> IO ()
addHotLocation cm ot k = do
  hotLocs <- takeMVar $ cm^.hotLocsMVar
  putMVar (cm^.hotLocsMVar) $ S.insert (ot,k) hotLocs

cacheMgrCore :: CacheManager -> IO ()
cacheMgrCore cm = forever $ do
  takeMVar $ cm^.semMVar
  -- Woken up. Read the current list of hot locations, and empty the MVar.
  locs <- takeMVar $ cm^.hotLocsMVar
  putMVar (cm^.hotLocsMVar) S.empty
  t1 <- getCurrentTime
  objTypes <- readMVar $ cm^.objectTypes
  vc <- readMVar (cm^.inMemVClock)
  shimId <- readMVar (cm^.shimId)
  let Just seqNoList = M.lookup shimId vc
  let shimids = [0..cNUM_REPLICAS]
  let vcList = Prelude.zip shimids seqNoList
  let vcMap = M.fromList vcList
  objKeyList <- foldM (\list ot -> do
                            objectRows <- runCas (cm^.pool) $ cqlReadObjectKeys ot
                            return $ list ++ (f (ot, objectRows) vcMap)) [] $ S.toList objTypes
  let newLocs = S.union locs (S.fromList objKeyList)
  --putStrLn $ show newLocs
  -- Fetch updates
  t2 <- getCurrentTime
  --putStrLn $ "Reading " ++ show (S.size newLocs) ++" keys took : " ++ show (diffUTCTime t2 t1)
  fetchUpdates cm ONE $ S.toList newLocs
  t3 <- getCurrentTime
  --putStrLn $ "Fetching updates took : " ++ show (diffUTCTime t3 t2)
  -- Wakeup threads that are waiting for the cache to be refreshed
  blockedList <- takeMVar $ cm^.blockedMVar
  putMVar (cm^.blockedMVar) []
  mapM_ (\mv -> putMVar mv ()) blockedList
  where
    f :: (ObjType, [ObjectKeyRow]) -> M.Map ShimID SeqNo -> [(ObjType, Key)]
    f (i, xs) vcMap = foldl (\list x@(key, shimID, seqNo) -> 
                                let Just rSeqNo = M.lookup shimID vcMap in
                                if rSeqNo < seqNo then (i, key):list
                                else list) [] xs

-- Print stats
printStats :: CacheManager -> ObjType -> Key -> IO ()
printStats cm ot k = do
  cacheMap <- readMVar $ cm^.cacheMVar
  cursorMap <- readMVar $ cm^.cursorMVar
  depsMap <- readMVar $ cm^.depsMVar
  lgca <- readMVar $ cm^.lastGCAddrMVar
  (inclTxns,_) <- readMVar $ cm^.includedTxnsMVar
  hwm <- readMVar $ cm^.hwmMVar
  hotLocs <- readMVar $ cm^.hotLocsMVar
  tq <- readMVar $ cm^.blockedMVar

  let cache = case M.lookup (ot,k) cacheMap of {Nothing -> S.empty; Just s -> s}
  let cursor = case M.lookup (ot,k) cursorMap of {Nothing -> M.empty; Just s -> s}
  let deps = case M.lookup (ot,k) depsMap of {Nothing -> S.empty; Just s -> s}

  putStrLn $ "Stats : cache=" ++ show (S.size cache) ++ " cursor=" ++ show (M.size cursor) ++
             " deps=" ++ show (S.size deps) ++ " lgca=" ++ show (M.size lgca) ++
             " incTxns=" ++ show (S.size inclTxns) ++ " hwm=" ++ show (M.size hwm) ++
             " hotLocs=" ++ show (S.size hotLocs) ++ " tq=" ++ show (length tq)

-- Returns the set of effects at the location and a set of nearest dependencies
-- for this location.
getContext :: CacheManager -> ObjType -> Key -> IO ([Effect], S.Set Addr)
getContext cm ot k = do
  !cache <- takeMVar $ cm^.cacheMVar
  !deps <- takeMVar $ cm^.depsMVar
  putMVar (cm^.cacheMVar) cache
  putMVar (cm^.depsMVar) deps
  let !v1 = case M.lookup (ot,k) cache of
             Nothing -> []
             Just s -> Prelude.map (\(a,e) -> e) (S.toList s)
  let !v2 = case M.lookup (ot,k) deps of {Nothing -> S.empty; Just s -> s}
  -- printStats cm ot k
  return (v1, v2)

writeEffect :: CacheManager -> ObjType -> Key -> Addr -> Effect -> S.Set Addr
            -> Consistency -> Maybe TxnID -> IO ()
writeEffect cm ot k addr eff deps const mbtxnid = do
  putStrLn $ show ot ++ " " ++ show k
  let Addr sid sqn = addr
  shimId <- readMVar $ cm^.shimId
  -- Does cache include the previous effect?
  isPrevEffectAvailable <- doesCacheInclude cm ot k sid (sqn - 1)
  let isTxn = case mbtxnid of {Nothing -> False; otherwise -> True}
  -- Only write to cache if the previous effect is available in the cache. This
  -- maintains the cache to be a causally consistent cut of the updates. But do
  -- not update cache if the effect is in a transaction. This prevents
  -- uncommitted effects from being made visible.
  takeMVar $ (cm^.boundMVar)
  vc <- readMVar $ cm^.inMemVClock
  kBound <- readMVar $ cm^.kBound
  --putStrLn $ "Trying to write (" ++ show sid ++ ", " ++ show sqn ++ ") from Shim # " ++ show shimId ++" on " ++ show k 
  let seqnoList = getCollumnByShimId shimId vc
  if (seqnoList !! shimId) < (Prelude.minimum seqnoList) + kBound--cK_BOUND
    then do
    if ((not isTxn) && (sqn == 1 || isPrevEffectAvailable))
      then do
        !cache <- takeMVar $ cm^.cacheMVar
        !cursor <- takeMVar $ cm^.cursorMVar
        -- curDeps may be different from the deps seen before the operation was performed.
        !curDeps <- takeMVar $ cm^.depsMVar
        -- Update cache
        let !newCache = M.insertWith S.union (ot,k) (S.singleton (addr, eff)) cache
        putMVar (cm^.cacheMVar) newCache
        -- Update in memory vector clock & replica seq no.
        counter_val <- takeMVar $ cm^.opCount
        putMVar (cm^.opCount) (counter_val + 1)
        putStrLn "Reached here"
        vc <- takeMVar (cm^.inMemVClock)
        newVc <- updateInMemVC shimId (counter_val+1) vc shimId
        putMVar (cm^.inMemVClock) newVc
        -- Update cursor
        let !cursorAtKey = case M.lookup (ot,k) cursor of {Nothing -> M.empty; Just m -> m}
        let !newCursorAtKey = M.insert sid sqn cursorAtKey
        putMVar (cm^.cursorMVar) $ M.insert (ot,k) newCursorAtKey cursor
        -- Update dependence
        -- the deps seen by the effect is a subset of the curDeps. We over
        -- approximate the dependence set; only means that effects might take longer
        -- to converge, but importantly does not affect correctness.
        let newDeps = case M.lookup (ot,k) curDeps of {Nothing -> S.empty; Just s -> s}
        putMVar (cm^.depsMVar) $ M.insert (ot,k) (S.singleton addr) curDeps
        -- Write to database
        runCas (cm^.pool) $ cqlInsert ot const k (sid, sqn, newDeps, EffectVal eff, mbtxnid, shimId, counter_val + 1)
        putMVar (cm^.boundMVar) ()
    else do
      -- Write to database
      counter_val <- takeMVar $ cm^.opCount
      putMVar (cm^.opCount) (counter_val + 1)
      vc <- takeMVar (cm^.inMemVClock)
      newVc <- updateInMemVC shimId (counter_val+1) vc shimId
      putMVar (cm^.inMemVClock) newVc
      runCas (cm^.pool) $ cqlInsert ot const k (sid, sqn, deps, EffectVal eff, mbtxnid, shimId, counter_val + 1)
      putMVar (cm^.boundMVar) ()
  else do
    --putStrLn "K-Bound reached!"
    kBoundedCount <- takeMVar $ cm^.kBoundedOpsCount
    let newKBoundedCount = kBoundedCount + 1
    putMVar (cm^.kBoundedOpsCount) newKBoundedCount
    putMVar (cm^.boundMVar) ()
    waitForCacheRefresh cm ot k
    writeEffect cm ot k addr eff deps const mbtxnid

getCollumnByShimId :: ShimID -> InMemoryVC -> [SeqNo]
getCollumnByShimId shimId inMemVClock =
  let seqnoLists = map snd $ M.toList inMemVClock
  in map (f shimId) seqnoLists 
  where
    f :: ShimID -> [SeqNo] -> SeqNo
    f shimId seqnoList = seqnoList !! shimId

updateInMemVC :: ShimID {-Remote ShimID-} -> SeqNo{-New seq no.-} -> InMemoryVC -> ShimID {-ShimID-} -> IO (InMemoryVC)
updateInMemVC rshimId counter_val inMemClock shimId = do
  let Just seqnoList = M.lookup shimId inMemClock
  if seqnoList !! rshimId < counter_val then do
    return $ M.insert shimId (replaceNth rshimId counter_val seqnoList) inMemClock
  else do
    return inMemClock
  where
    replaceNth n newVal (x:xs)
         | n == 0 = newVal:xs
         | otherwise = x:replaceNth (n-1) newVal xs

doesCacheInclude :: CacheManager -> ObjType -> Key -> SessID -> SeqNo -> IO Bool
doesCacheInclude cm ot k sid sqn = do
  cursor <- readMVar $ cm^.cursorMVar
  case M.lookup (ot,k) cursor of
    Nothing -> return False
    Just cursorAtKey ->
      case M.lookup sid cursorAtKey of
        Nothing -> return False
        Just curSqn -> return $ (==) sqn curSqn

isCoveredInReplica :: CacheManager -> SeqNo -> IO Bool
isCoveredInReplica cm seqno = do
  inMemVClock <- readMVar $ cm^.inMemVClock
  currShimId <- readMVar $ cm^.shimId
  let Just seqnoList = M.lookup currShimId inMemVClock
  return $ (seqnoList !! currShimId) >= (seqno - 1)

waitForCacheRefresh :: CacheManager -> ObjType -> Key -> IO ()
waitForCacheRefresh cm ot k = do
  hotLocs <- takeMVar $ cm^.hotLocsMVar
  blockedList <- takeMVar $ cm^.blockedMVar
  mv <- newEmptyMVar
  putMVar (cm^.hotLocsMVar) $ S.insert (ot,k) hotLocs
  putMVar (cm^.blockedMVar) $ mv:blockedList
  takeMVar mv

includedTxns :: CacheManager -> IO (S.Set TxnID)
includedTxns cm = do
  txns <- readMVar (cm^.includedTxnsMVar)
  return $ sel1 txns

doesCacheIncludeTxns :: CacheManager -> S.Set TxnID -> IO Bool
doesCacheIncludeTxns cm deps = do
  incl <- includedTxns cm
  return $ deps `S.isSubsetOf` incl

fetchTxns :: CacheManager -> S.Set TxnID -> IO ()
fetchTxns cm deps = do
  incl <- includedTxns cm
  let diffSet = S.difference deps incl
  objs <- foldM (\acc txid -> do
            objs <- getObjs txid
            return $ S.union acc objs) S.empty $ S.toList diffSet
  fetchUpdates cm ONE (S.toList objs)
  where
    getObjs txid = do
      res <- runCas (cm^.pool) $ readTxn txid
      case res of
        Nothing -> return $ S.empty
        Just s -> return $ S.map (\(TxnDep ot k _ _) -> (ot,k)) s

snapshotCache :: CacheManager -> IO CacheMap
snapshotCache cm = do
  readMVar $ cm^.cacheMVar
