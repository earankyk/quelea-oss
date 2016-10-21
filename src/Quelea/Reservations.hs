{-# LANGUAGE ScopedTypeVariables, EmptyDataDecls, TemplateHaskell,
    DataKinds, OverloadedStrings, DoAndIfThenElse  #-}

--Example with 3 replicas
module Quelea.Reservations (
	getCollumnByShimId,
	--updateInMemVC,
	vcCronJob,
	isCoveredInReplica
) where

import Quelea.Types
import qualified Data.Map as M
import Quelea.DBDriver


getCollumnByShimId :: ShimID {-ShimID-} -> InMemoryVC -> [Int] {-collumn corresponding to shimId-}
getCollumnByShimId shimId inMemVClock =
	let seqnoMap = map f inMemVClock
	in map snd $ M.toList seqnoMap
	where
		f :: [Int] -> Int {-ShimID-} -> Int
		f seqnoList shimId = seqnoList !! shimId

{-updateInMemVC :: ShimID {-Remote ShimID-} -> Int{-New seq no.-} -> InMemoryVC -> ShimID {-ShimID-} -> InMemoryVC
updateInMemVC rshimId counter_val inMemVClock shimId = 
	let seqnoList = M.lookup shimId inMemVClock
	if $ (seqnoList !! rshimId) < counter_val 
		then M.insert shimId (replaceNth rshimId counter_val seqnoList) inMemVClock
		else inMemVClock
	where
	 	replaceNth n newVal (x:xs)
		     | n == 0 = newVal:xs
		     | otherwise = x:replaceNth (n-1) newVal xs-}

-- cronJob to write in-memory vector clock to table, and update it with entries for other replicas
	
isCoveredInReplica :: Int {-ShimID of current replica-} -> InMemoryVC 
                       Int {-ShimID in which effect was applied-}-> Int{-new seqno.-} -> Bool
isCoveredInReplica currShimId inMemVClock remoteShimId seqno =
	let seqnoList = M.lookup currShimId inMemVClock
	in (seqnoList !! remoteShimId) <= seqno - 1

{-- Goes into Cache.hs 
	writeEffect :: CacheManager -> ObjType -> Key -> Addr -> Effect -> S.Set Addr
            -> Consistency -> Maybe TxnID -> IO ()
		-- When updating cache update in-memory vector clock
			let seqnoList = getCollumnByShimId shimId inMemVClock
			if seqnoList !! shimId <= minimum(seqnoList) + k/n then do
					-- Update cache
			    let !newCache = M.insertWithKey S.union (ot,k) (S.singleton (addr, eff)) cache
			    putMVar (cm^.cacheMVar) newCache
			    if isCoveredInReplica shimId inMemVClock rshimid counter_val then do
					let inMemVClock = M.insertWithKey (updateInMemVC rshimid counter_val inMemVClock) shimId inMemVClock
			else do
				waitForCacheRefresh cache ot k
				writeEffect blah blah blah
			cqlReservationIncCounter
			counter_val <- cqlReservationReadCounter
			runCas (cm^.pool) $ cqlInsert (ot const k) (sid, sqn, deps, EffectVal eff, mbtxnid,{-New additions-}shimId, counter_val)-}

-- Goes into UpdateFetcher.hs
	-- Once we have built the cursor, filter those rows which are covered.
{-	  let effRowsMap = M.foldlWithKey (\erm (ot,k) rowList ->
	         let er = foldl (\er (sid,sqn,deps,val,mbTxid,rshimid,counter_val) ->
	                case val of
	                  EffectVal bs ->
	                    if isCovered newCursor ot k sid sqn then 
	                    	-- Only update In Memory VC for set of effects which are covered
	                    	if isCoveredInReplica shimId inMemVClock rshimid counter_val then
	                    		let inMemVClock = updateInMemVC rshimid counter_val inMemVClock shimId
	                    	-----------------------------------------------------------------	
	                    	er
	                    else
	                      let mkRetVal txid txdeps = M.insert (Addr sid sqn) (NotVisited bs deps txid txdeps) er
	                      in case mbTxid of
	                           Nothing -> mkRetVal Nothing S.empty
	                           Just txid -> case M.lookup txid newTransMap of
	                                          Nothing -> mkRetVal (Just txid) S.empty {- Eventual consistency (or) txn in progress! -}
	                                          Just txnDeps -> mkRetVal (Just txid) txnDeps
	                  GCMarker _ -> er) M.empty rowList
	         in M.insert (ot,k) er erm) M.empty rowsMapCRS-}