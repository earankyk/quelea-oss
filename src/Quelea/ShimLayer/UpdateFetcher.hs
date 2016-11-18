{-# LANGUAGE ScopedTypeVariables, TemplateHaskell, DoAndIfThenElse, BangPatterns, FlexibleContexts #-}

module Quelea.ShimLayer.UpdateFetcher (
  fetchUpdates
) where

import Control.Concurrent.MVar
import qualified Data.Map as M
import qualified Data.Set as S
import Control.Lens
import Database.Cassandra.CQL
import Control.Monad.Trans.State
import Control.Monad.Trans (liftIO)
import Control.Monad (mapM_, when, foldM)
import Data.ByteString (empty)
import Data.Maybe (fromJust)
import Data.List
import Control.Concurrent (myThreadId)
import Control.Arrow (second)
import Debug.Trace
import System.IO (hFlush, stdout)
import System.Posix.Process (getProcessID)
import Data.Tuple.Select
import Data.Time
import Data.Int

import Quelea.Consts
import Quelea.Types
import Quelea.ShimLayer.Types
import Quelea.DBDriver

makeLenses ''CacheManager
makeLenses ''Addr

data CollectRowsState = CollectRowsState {
  _inclTxnsCRS :: S.Set TxnID,
  _todoObjsCRS :: S.Set (ObjType, Key),
  _rowsMapCRS  :: M.Map (ObjType, Key) [ReadRow],
  _newTxnsCRS  :: M.Map TxnID (S.Set TxnDep)
}

makeLenses ''CollectRowsState
makeLenses ''TxnDep

data VisitedState = Visited (Maybe (Effect, ShimID, SeqNo, Maybe TxnID))
                  | NotVisited { effect  :: Effect,
                                 deps    :: S.Set Addr,
                                 shimid  :: ShimID,
                                 rseqno  :: SeqNo,
                                 txnid   :: Maybe TxnID,
                                 txndeps :: S.Set TxnDep }
                  | Visiting deriving Show

data ResolutionState = ResolutionState {
  _visitedState :: M.Map (ObjType, Key) (M.Map Addr VisitedState)
}

makeLenses ''ResolutionState

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

data CacheUpdateState = CacheUpdateState {
  _cacheCUS      :: CacheMap,
  _cursorCUS     :: CursorMap,
  _depsCUS       :: NearestDepsMap,
  _lastGCAddrCUS :: M.Map (ObjType, Key) SessID,
  _lastGCTimeCUS :: M.Map (ObjType, Key) UTCTime,
  _inclTxnsCUS   :: (S.Set TxnID, M.Map (ObjType, Key) (S.Set TxnID))
}

makeLenses ''CacheUpdateState

fetchUpdates :: CacheManager -> Consistency -> [(ObjType, Key)] -> IO ()
fetchUpdates cm const todoList = do
  shimid <- readMVar $ cm^.shimId
  -- Recursively read the DB and collect all the rows
  !cursor <- readMVar $ cm^.cursorMVar
  !inclTxns <- readMVar $ cm^.includedTxnsMVar
  !lgctMap <- readMVar $ cm^.lastGCTimeMVar

  let todoObjsCRS = S.fromList todoList
  let crs = CollectRowsState (sel1 inclTxns) todoObjsCRS M.empty M.empty
  CollectRowsState _ _ !rowsMapCRS !newTransMap <- execStateT (collectTransitiveRows (cm^.pool) const lgctMap) crs

  --putStrLn $ "Initial : " ++ show (M.size rowsMapCRS)

  vc <- takeMVar $ cm^.inMemVClock
  let Just seqNoList = M.lookup shimid vc
  let shimOpList = map (\(sid,sqn,deps,val,mbTxid,rshimid,rseqno) -> (rshimid, rseqno)) (foldl (++) [] $ map snd $ M.toList rowsMapCRS)
  let shimOpMap = convertKVList shimOpList
  let highestOpMap = M.foldlWithKey (\highestOpMap1 rshimId list -> 
          M.insert rshimId (processSeqNoList list (seqNoList !! rshimId) shimid rshimId) highestOpMap1) M.empty shimOpMap
  let newVc = M.foldlWithKey (\vc rshimid highestOp -> 
          if rshimid /= shimid then
            updateInMemVC rshimid highestOp vc shimid
          else vc) vc highestOpMap 
  let y = M.foldlWithKey (\gcmm (ot,k) rowList ->
          let gcm = foldl (\gcm x@(_,_,_,_,_,rshimid,rseqno) ->
                  if rshimid /= shimid then
                    let Just highestOp = M.lookup rshimid highestOpMap in
                    if rseqno <= highestOp then x:gcm else gcm
                  else x:gcm) [] rowList
          in M.insert (ot,k) gcm gcmm) M.empty rowsMapCRS
  let !rowsMapCRS = y
  putMVar (cm^.inMemVClock) newVc
  --putStrLn $ "Final : " ++ show (M.size y)

  -- Update disk row count for later use by gcDB
  drc <- takeMVar $ cm^.diskRowCntMVar
  let newDrc = M.foldlWithKey (\iDrc (ot,k) rows -> M.insert (ot,k) (length rows) iDrc) drc rowsMapCRS
  putMVar (cm^.diskRowCntMVar) newDrc

  --putStrLn $ "Fetched " ++ show (length $ M.toList rowsMapCRS) ++" updates on Shim # " ++ show shimid

  -- First collect the GC markers
  let gcMarkerMap = M.foldlWithKey (\gcmm (ot,k) rowList ->
         let gcm = foldl (\gcm (sid::SessID,sqn,deps,val,mbTxid,_,_) ->
                case val of
                  EffectVal bs -> gcm
                  GCMarker atTime ->
                    case gcm of
                      Nothing -> Just (sid, sqn, deps, atTime)
                      Just _ -> error "Multiple GC Markers") Nothing rowList
         in M.insert (ot,k) gcm gcmm) M.empty rowsMapCRS

  -- Build new cursor with GC markers. The idea here is that if we did find a
  -- GC marker & we have not seen this marker already, then keep hold of all
  -- the rows except the ones explicitly covered by the GC. The reason is that
  -- subsequently, we will clear our cache and rebuild it. Here, it is not
  -- correct to ignore those rows, which might have already been seen, but was
  -- not GCed.
  !lastGCAddrMap <- readMVar $ cm^.lastGCAddrMVar
  let !newCursor = M.foldlWithKey (\c (ot,k) gcMarker ->
                    let lastGCAddr = M.lookup (ot,k) lastGCAddrMap
                    in if newGCHasOccurred lastGCAddr gcMarker
                         then case gcMarker of
                                Nothing -> c
                                Just marker ->
                                  let gcCursor = buildCursorFromGCMarker marker
                                  in M.insert (ot,k) gcCursor c
                         else c)
                     cursor gcMarkerMap

  -- Once we have built the cursor, filter those rows which are covered.
  let effRowsMap = M.foldlWithKey (\erm (ot,k) rowList ->
         let er = foldl (\er (sid,sqn,deps,val,mbTxid,shimid,rseqno) ->
                case val of
                  EffectVal bs ->
                    if isCovered newCursor ot k sid sqn
                    then er
                    else
                      let mkRetVal txid txdeps = M.insert (Addr sid sqn) (NotVisited bs deps shimid rseqno txid txdeps) er
                      in case mbTxid of
                           Nothing -> mkRetVal Nothing S.empty
                           Just txid -> case M.lookup txid newTransMap of
                                          Nothing -> mkRetVal (Just txid) S.empty {- Eventual consistency (or) txn in progress! -}
                                          Just txnDeps -> mkRetVal (Just txid) txnDeps
                  GCMarker _ -> er) M.empty rowList
         in M.insert (ot,k) er erm) M.empty rowsMapCRS

  {--- Update in-memory vector clock
  shimid <- readMVar $ cm^.shimId
  --takeMVar $ (cm^.boundMVar)
  vc <- takeMVar $ cm^.inMemVClock
  let newVc = M.foldlWithKey (\vc (ot,k) rowList -> 
                  foldl (\vc (sid,sqn,deps,val,mbTxid,rshimid,rseqno) -> 
                    case val of
                      EffectVal bs -> 
                        if rshimid /= shimid then {-trace ("Shim # "++show shimid++" saw ("++show sid++", "++show sqn++")")-} updateInMemVC rshimid rseqno vc shimid
                        else vc
                      otherwise -> vc) vc rowList) vc rowsMapCRS
  putMVar (cm^.inMemVClock) newVc
  --putMVar (cm^.boundMVar) ()-}

  -- debugPrint $ "newCursor"
  -- mapM_ (\((ot,k), m) -> mapM_ (\(sid,sqn) -> debugPrint $ show $ Addr sid sqn) $ M.toList m) $ M.toList newCursor

  -- Now filter those rows which are unresolved i.e) those rows whose
  -- dependencies are not visible.
  let !filteredMap = filterUnresolved newCursor effRowsMap

  -- debugPrint $ "filteredMap"
  -- mapM_ (\((ot,k), s) -> mapM_ (\(addr,_,_) -> debugPrint $ show addr) $ S.toList s) $ M.toList filteredMap

  -- Update state. First obtain locks...
  !cache      <- takeMVar $ cm^.cacheMVar
  !cursor     <- takeMVar $ cm^.cursorMVar
  !deps       <- takeMVar $ cm^.depsMVar
  !lastGCAddr <- takeMVar $ cm^.lastGCAddrMVar
  !lastGCTime <- takeMVar $ cm^.lastGCTimeMVar
  !inclTxns   <- takeMVar $ cm^.includedTxnsMVar

  let core =
        mapM_ (\((ot,k), filteredSet) ->
          updateCache ot k filteredSet
            (case M.lookup (ot,k) newCursor of {Nothing -> M.empty; Just m -> m})
            (case M.lookup (ot,k) gcMarkerMap of {Nothing -> error "fetchUpdates(1)"; Just x -> x})) $ M.toList filteredMap

  let CacheUpdateState !newCache !new2Cursor !newDeps !newLastGCAddr !newLastGCTime !newInclTxns =
        execState core (CacheUpdateState cache cursor deps lastGCAddr lastGCTime inclTxns)

  -- debugPrint $ "finalCursor"
  -- mapM_ (\((ot,k), m) -> mapM_ (\(sid,sqn) -> debugPrint $ show $ Addr sid sqn) $ M.toList m) $ M.toList new2Cursor

  -- Flush cache if necessary {- Catch: XXX KC: A query cannot desire to see more than 1024 objects -}
  if M.size newCache < cCACHE_MAX_OBJS
  then do
    putMVar (cm^.cacheMVar) newCache
    putMVar (cm^.cursorMVar) new2Cursor
    putMVar (cm^.depsMVar) newDeps
    putMVar (cm^.lastGCAddrMVar) newLastGCAddr
    putMVar (cm^.lastGCTimeMVar) newLastGCTime
    putMVar (cm^.includedTxnsMVar) newInclTxns
  else do
    -- Reset almost everything
    putMVar (cm^.cacheMVar) M.empty
    putMVar (cm^.cursorMVar) M.empty
    putMVar (cm^.depsMVar) M.empty
    putMVar (cm^.lastGCAddrMVar) M.empty
    putMVar (cm^.lastGCTimeMVar) M.empty
    putMVar (cm^.includedTxnsMVar) (S.empty, M.empty)

    takeMVar (cm^.hwmMVar)
    putMVar (cm^.hwmMVar) M.empty
    takeMVar (cm^.diskRowCntMVar)
    putMVar (cm^.diskRowCntMVar) M.empty
    takeMVar (cm^.hotLocsMVar)
    putMVar (cm^.hotLocsMVar) S.empty

    -- fetch updates again
    fetchUpdates cm const todoList
  where
    buildCursorFromGCMarker (sid, sqn, deps,_) =
      S.foldl (\m (Addr sid sqn) ->
            case M.lookup sid m of
              Nothing -> M.insert sid sqn m
              Just oldSqn -> M.insert sid (max oldSqn sqn) m) (M.singleton sid sqn) deps
    updateCache ot k filteredSet gcCursor gcMarker = do
      -- Handle GC
      lgca <- use lastGCAddrCUS
      let cacheGCId = M.lookup (ot,k) lgca
      -- If a new GC has occurred, flush the cache and get the new effects
      -- inserted by the GC.
      when (newGCHasOccurred cacheGCId gcMarker) $ do
        -- Update GC information
        let (newGCSessID, _, _, atTime) =
              case gcMarker of
                Nothing -> error "fetchUpdates(2)"
                Just x -> x
        lgca <- use lastGCAddrCUS
        lastGCAddrCUS .= M.insert (ot,k) newGCSessID lgca
        lgct <- use lastGCTimeCUS
        lastGCTimeCUS .= M.insert (ot,k) atTime lgct
        -- empty cache
        cache <- use cacheCUS
        cacheCUS .= M.insert (ot,k) S.empty cache
        -- reset cursor
        cursor <- use cursorCUS
        cursorCUS .= M.insert (ot,k) M.empty cursor
        -- reset deps
        deps <- use depsCUS
        depsCUS .= M.insert (ot,k) S.empty deps

      -- Update cache
      cache <- use cacheCUS
      let newEffs :: CacheMap = M.singleton (ot,k) (S.map (\(a,e,_,_,_) -> (a,e)) filteredSet)
      cacheCUS .= M.unionWith S.union cache newEffs
      
      -- Update cursor
      cursor <- use cursorCUS
      let cursorAtKey = case M.lookup (ot, k) cursor of
                          Nothing -> gcCursor
                          Just m -> mergeCursorsAtKey m gcCursor
      let newCursorAtKey = S.foldl (\m (Addr sid sqn, _, _, _, _) ->
                              case M.lookup sid m of
                                Nothing -> M.insert sid sqn m
                                Just oldSqn -> if oldSqn < sqn
                                              then M.insert sid sqn m
                                              else m) cursorAtKey filteredSet
      cursorCUS .= M.insert (ot,k) newCursorAtKey cursor

      -- Update dependence
      deps <- use depsCUS
      let curDepsMap = case M.lookup (ot,k) deps of
                         Nothing -> M.empty
                         Just s -> S.foldl (\m (Addr sid sqn) ->
                                     case M.lookup sid m of
                                       Nothing -> M.insert sid sqn m
                                       Just oldSqn -> if oldSqn < sqn
                                                      then M.insert sid sqn m
                                                      else m) M.empty s
      let maxSqnMap = S.foldl (\m (Addr sid sqn,_,_,_,_) ->
                                  case M.lookup sid m of
                                    Nothing -> M.insert sid sqn m
                                    Just oldSqn -> if oldSqn < sqn
                                                   then M.insert sid sqn m
                                                   else m) curDepsMap filteredSet
      -- Just convert "Map sid sqn" to "Set (Addr sid sqn)"
      let maxSqnSet = M.foldlWithKey (\s sid sqn -> S.insert (Addr sid sqn) s) S.empty maxSqnMap
      -- Insert into deps to create newDeps
      -- OLD CODE : let newDeps = M.unionWith S.union deps $ M.singleton (ot,k) maxSqnSet
      let newDeps = M.insert (ot,k) maxSqnSet deps
      depsCUS .= newDeps

      -- Update included transactions
      (inclTxnsSet, inclTxnsMap) <- use inclTxnsCUS
      let newTxns = S.foldl (\acc (_,_,_,_,mbTxid) ->
                        case mbTxid of
                          Nothing -> acc
                          Just txid -> S.insert txid acc) S.empty filteredSet
      let newInclTxns = (S.union inclTxnsSet newTxns, M.insertWith S.union (ot,k) newTxns inclTxnsMap)
      inclTxnsCUS .= newInclTxns 

    sortGT (a1,e1,_,_,_,shimid1,rseqno1) (a2,e2,_,_,_,shimid2,rseqno2)
        | shimid1 > shimid2 = GT
        | shimid1 < shimid2 = LT
        | shimid1 == shimid2 = compare rseqno1 rseqno2

    newGCHasOccurred :: Maybe SessID -> Maybe (SessID, SeqNo, S.Set Addr, UTCTime) -> Bool
    newGCHasOccurred Nothing Nothing = False
    newGCHasOccurred Nothing (Just _) = True
    newGCHasOccurred (Just _) Nothing = error "newGCHasOccurred: unexpected state"
    newGCHasOccurred (Just fromCache) (Just (fromDB,_,_,_)) = fromCache /= fromDB

    convertKVList :: Ord a => [(a, b)] -> M.Map a [b]
    convertKVList = M.fromListWith (++) . map (second (:[]))

    processSeqNoList :: [SeqNo] -> SeqNo -> ShimID -> ShimID -> SeqNo 
    processSeqNoList seqNoList1 vcSeqNo shimid rshimid = 
      let seqNoList = filter (> vcSeqNo) seqNoList1 in
      let indexList = [(1::Int64)..(fromIntegral(length seqNoList)::Int64)] in
      let m = take (length seqNoList) $ repeat vcSeqNo in
      let compareList = zipWith (+) indexList m in
      let compareTuple = zip compareList (sort seqNoList) in
      let numberOfEffects = {-trace (show compareTuple ++ "from " ++ show rshimid ++ " on " ++ show shimid)-} length $ takeWhile (\(seqNo1, seqNo2) -> seqNo1 == seqNo2) compareTuple in 
      if numberOfEffects == 0 then 
        if length seqNoList /= 0 then 
          trace ("Missed effect(s) on "++ show shimid) (vcSeqNo) 
        else vcSeqNo
      else ((sort seqNoList) !! (numberOfEffects - 1))

updateInMemVC :: ShimID {-Remote ShimID-} -> SeqNo{-New seq no.-} -> InMemoryVC -> ShimID {-ShimID-} -> InMemoryVC
updateInMemVC rshimId counter_val inMemClock shimId =
  let Just seqnoList = M.lookup shimId inMemClock in
  if counter_val > seqnoList !! rshimId then
    M.insert shimId (replaceNth rshimId counter_val seqnoList) inMemClock
  else inMemClock
  where
    replaceNth n newVal (x:xs)
         | n == 0 = newVal:xs
         | otherwise = x:replaceNth (n-1) newVal xs

isCovered :: CursorMap -> ObjType -> Key -> SessID -> SeqNo -> Bool
isCovered cursor ot k sid sqn =
  case M.lookup (ot,k) cursor of
    Nothing -> False
    Just cursorAtKey ->
      case M.lookup sid cursorAtKey of
        Nothing -> False
        Just curSqn -> sqn <= curSqn

filterUnresolved :: CursorMap
                 -> M.Map (ObjType, Key) (M.Map Addr VisitedState)
                 -> M.Map (ObjType, Key) (S.Set (Addr, Effect, ShimID, SeqNo, Maybe TxnID))
filterUnresolved cm vs1 =
  let core = mapM_ (\((ot,k), vsObj) ->
               mapM_ (\(Addr sid sqn, _) ->
                 resolve cm ot k sid sqn) $ M.toList vsObj) $ M.toList vs1
      ResolutionState vs2 = execState core (ResolutionState vs1)
  in M.map (M.foldlWithKey (\s addr vs ->
       case vs of
         Visited (Just (eff, shimid, rseqno, mbTxid)) -> S.insert (addr, eff, shimid, rseqno, mbTxid) s
         otherwise -> s) S.empty) vs2

resolve :: CursorMap -> ObjType -> Key -> SessID -> SeqNo -> State ResolutionState Bool
resolve cursor ot k sid sqn = do
  vs <- lookupVisitedState cursor ot k sid sqn
  case vs of
    Visited Nothing -> return False
    Visited (Just _) -> return True
    Visiting -> return True
    NotVisited eff deps shimid rseqno mbTxid txnDeps -> do
      -- First mark this node as visiting
      updateVisitedState ot k sid sqn Visiting
      -- Process local dependences
      res1 <- foldM (\acc (Addr sid sqn) ->
                resolve cursor ot k sid sqn >>= return . ((&&) acc)) True $ S.toList deps
      -- Process remote dependences
      res2 <- case mbTxid of
        Nothing -> return res1
        Just txid ->
          if S.size txnDeps == 0
          then return False {- Txn in progress (or) eventual consistency -}
          else foldM (\acc (TxnDep ot k sid sqn) ->
                 resolve cursor ot k sid sqn >>= return . ((&&) acc)) res1 $ S.toList txnDeps
      -- Update final state
      if res2
      then updateVisitedState ot k sid sqn (Visited $ Just (eff, shimid, rseqno, mbTxid))
      else updateVisitedState ot k sid sqn (Visited Nothing)
      return res2
  where
    trueVal = Visiting
    falseVal = Visited Nothing
    lookupVisitedState cursor ot k sid sqn | sqn == 0 =
      return trueVal
    lookupVisitedState cursor ot k sid sqn | sqn > 0 = do
      if isCovered cursor ot k sid sqn
      then return trueVal
      else do
        vs <- use visitedState
        case M.lookup (ot,k) vs of
          Nothing -> return falseVal
          Just vsObj -> case M.lookup (Addr sid sqn) vsObj of
                          Nothing -> return $ Visited Nothing
                          Just val -> return $ val
    updateVisitedState ot k sid sqn val = do
      vs <- use visitedState
      let newVsObj = case M.lookup (ot,k) vs of
            Nothing -> M.insert (Addr sid sqn) val M.empty
            Just vsObj -> M.insert (Addr sid sqn) val vsObj
      visitedState .= M.insert (ot,k) newVsObj vs

-- Combines curors at a particular key. Since a cursor at a given (key, sessid)
-- records the largest sequence number seen so far, given two cursors at some
-- key k, the merge operation picks the larger of the sequence numbers for each
-- sessid.
mergeCursorsAtKey :: CursorAtKey -> CursorAtKey -> CursorAtKey
mergeCursorsAtKey = M.unionWith max

collectTransitiveRows :: Pool -> Consistency
                      -> M.Map (ObjType, Key) UTCTime
                      -> StateT CollectRowsState IO ()
collectTransitiveRows pool const lgct = do
  to <- use todoObjsCRS
  case S.minView to of
    Nothing -> return ()
    Just (x@(ot,k), xs) -> do
      -- Update todo list
      todoObjsCRS .= xs
      rm <- use rowsMapCRS
      case M.lookup x rm of
        Just _ -> return ()
        Nothing -> do -- Work
          -- Read this (ot,k)
          rows <- case M.lookup (ot,k) lgct of
                  Nothing -> liftIO $ do
                    runCas pool $ cqlRead ot const k
                  Just gcTime -> liftIO $ do
                    runCas pool $ cqlReadAfterTime ot const k gcTime
          -- Mark as read
          rowsMapCRS .= M.insert (ot,k) rows rm 
          mapM_ processRow rows
      collectTransitiveRows pool const lgct
  where
    processRow (_,_,_,_,Nothing,_,_) = return ()
    processRow (sid, sqn, deps, val, Just txid,_,_) = do
      includedTxns <- use inclTxnsCRS
      -- Is this transaction id already included in the cache?
      when (not $ S.member txid includedTxns) $ do
        procTxns <- use newTxnsCRS
        -- Is this transaction already seen in this fetchUpdate?
        when (not $ M.member txid procTxns) $ do
          maybeDeps <- liftIO $ runCas pool $ readTxn txid
          case maybeDeps of
            Nothing -> -- Eventual consistency!
              return ()
            Just deps -> do
              newTxnsCRS .= M.insert txid deps procTxns
              mapM_ maybeAddObjToTodo $ S.toList deps
    maybeAddObjToTodo dep = do
      to <- use todoObjsCRS
      rm <- use rowsMapCRS
      let TxnDep ot k sid sqn = dep
      if S.member (ot,k) to || M.member (ot,k) rm
      then return ()
      else do
        todoObjsCRS .= S.insert (ot, k) to