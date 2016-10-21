{-# LANGUAGE TemplateHaskell, OverloadedStrings, DataKinds, TypeSynonymInstances,
             FlexibleInstances, BangPatterns, EmptyDataDecls, ScopedTypeVariables  #-}

module Codeec (
-- Types
EC, Storable(..), Ctxt, Key, Table,
Prop, Effect, Spec,

-- Table operations
createTable, dropTable, mkEC, runEC, printCtxt,

-- Spec builders
forall_, exists_, true, false, not_, (/\), (\/), (==>), sameEffect, vis, so,
sortOf, ite, sameAttr, distinctEffects, isInSameSess,

)
where

{-
 * TODO
 * ----
 * (1) A way to describe the store (tables) -- Essentially a datatype
 * definition whose values represent the write performed on the table.
 *
 * (2) A way to describe operations on the store (Stored procedures)
 * (2.1) The procedures are restricted to single-key.
 * (2.2) Can perform effects on the store.
 *
 * (3) A way to describe the proedure-level consistency.
 *
 * (4) A wat to describe global consistency.
 *
 * Tie together (3) and (4) with (1) and (2).
 -}

import Debug.Trace
import Data.Text (Text, pack)
import Language.Haskell.TH
import qualified Database.Cassandra.CQL as CQL
import Data.Graph.Inductive.Tree
import Data.Graph.Inductive.Graph
import Data.UUID
import Control.Lens hiding (Action, Index)
import Control.Monad.Trans.State
import Data.Data
import qualified Data.Set as S
import qualified Data.Map as M
import Data.Maybe
import Data.Serialize hiding (get, put)
import Control.Applicative
import Data.Int (Int64)
import Control.Monad.Trans (liftIO)
import Control.Monad (zipWithM, foldM)
import System.Random
import Control.Monad.IO.Class
import Control.Monad.Trans.Class (lift)
import Data.Foldable
import Types
import Spec


-------------------------------------------------------------------------------
-- Types
-------------------------------------------------------------------------------

type Table = String

data ECState = ECState {
                 _sess_EC  :: Sess,
                 _idx_EC   :: Index
               }

makeLenses ''ECState

type EC a = StateT ECState CQL.Cas a

instance CQL.CasType Addr where
  putCas (Addr x y) = do
    putLazyByteString $ toByteString x
    (putWord64be . fromIntegral) y
  getCas = do
    x <- fromJust . fromByteString <$> getLazyByteString 16
    y <- fromIntegral <$> getWord64be
    return $ Addr x y
  casType _ = CQL.CBlob

class (CQL.CasType a, Show a) => Storable a where

type Ctxt a = Gr (Sess, Index, a) ()

type RowValue a = (Key, Sess, Index, S.Set Addr, a)

data MkCtxtState a = MkCtxtState {
                     _hashMap :: M.Map Addr Node,
                     _ctxt :: Ctxt a
                    }

makeLenses ''MkCtxtState

-------------------------------------------------------------------------------
-- Internal
-------------------------------------------------------------------------------

-- Traverse helper
traverseAt :: (Applicative f, Ord k) => k -> (v -> f v) -> M.Map k v -> f (M.Map k v)
traverseAt k = at k . traverse

mkCtxtLoadNodes :: [RowValue a] -> State (MkCtxtState a) ()
mkCtxtLoadNodes [] = return ()
mkCtxtLoadNodes (x:xs) = do
  let (_, sess, aid, vis, value) = x
  let addr = Addr sess aid
  hm <- use hashMap
  x <- use ctxt
  let newId = M.size hm
  hashMap .= (at addr ?~ newId $ hm)
  ctxt .= insNode (newId, (sess, aid, value)) x
  mkCtxtLoadNodes xs

mkCtxtLoadEdges :: [RowValue a] -> State (MkCtxtState a) ()
mkCtxtLoadEdges [] = return ()
mkCtxtLoadEdges (x:xs) = do
  let (_,sess,aid,vis,_) = x
  let curAddr = Addr sess aid
  forM_ vis (\a -> processEdge a curAddr)
  mkCtxtLoadEdges xs
  where
    processEdge :: Addr -> Addr -> State (MkCtxtState a) ()
    processEdge a b = do
      hm <- use hashMap
      x <- use ctxt
      case (hm ^.at a, hm ^.at b) of
        (Just ai, Just bi) -> ctxt .= insEdge (ai, bi, ()) x
        otherwise -> return ()

mkCtxt :: Storable a => [RowValue a] -> Ctxt a
mkCtxt rows =
  {- Loading nodes first. There are the effects that are known. Then load the
   - edges. While loading edges, ignore the ones that originate from unknown
   - nodes.
   -}
  let comp = mkCtxtLoadNodes rows >> mkCtxtLoadEdges rows
      MkCtxtState _ ctxt = execState comp (MkCtxtState M.empty $ mkGraph [] [])
  in ctxt

mkVisSet :: Ctxt a -> Sess -> S.Set Addr
mkVisSet ctxt sess =
  let visSet = S.fromList $ map (\(_,(sess, at, _)) -> Addr sess at) $ labNodes ctxt
  in if S.size(visSet) == 0 then
       {- This is present in order to please Haskell Set to CQL set conversion.
        - There is seems to be a bug in the serialization of sets, which
        - encodes empty set as NULL (which is fine). When deserializing, we get
        - an error saying that the value is NULL, but the type is not Maybe!
        - Hence, subvert the error by never allowing the visSet to be empty.
        - Take away is that any effect with index 0 should be ignored.
        -}
       S.fromList [Addr sess 0]
     else visSet

-------------------------------------------------------------------------------
-- Core
-------------------------------------------------------------------------------

mkCreateTable :: Table -> CQL.Query CQL.Schema () ()
mkCreateTable tname = CQL.query $ pack $ "create table " ++ tname ++ " (key uuid, sess uuid, at bigint, vis set<blob>, value blob, primary key (key, sess, at)) "

mkDropTable :: Table -> CQL.Query CQL.Schema () ()
mkDropTable tname = CQL.query $ pack $ "drop table " ++ tname

mkInsert :: Table -> CQL.Query CQL.Write (RowValue a) ()
mkInsert tname = CQL.query $ pack $ "insert into " ++ tname ++ " (key, sess, at, vis, value) values (?, ?, ?, ?, ?)"

mkRead :: Table -> CQL.Query CQL.Rows (Key) (RowValue a)
mkRead tname = CQL.query $ pack $ "select key, sess, at, vis, value from " ++ tname ++ " where key=?"

createTable :: Table -> EC ()
createTable tname = liftIO . print =<< CQL.executeSchema CQL.ALL (mkCreateTable tname) ()

dropTable :: Table -> EC ()
dropTable tname = liftIO . print =<< CQL.executeSchema CQL.ALL (mkDropTable tname) ()

mkEC :: (Storable a, Show res)
      => (Ctxt a -> arg -> (res, Maybe a))
      -> Spec
      -> Table
      -> Key
      -> arg
      -> EC res
mkEC core spec tname k args = do
  -- Create the context
  rows <- CQL.executeRows CQL.ONE (mkRead tname) k
  let ctxt = mkCtxt rows
  -- Create the state for the stored procedure and execute it
  s <- use sess_EC
  a <- use idx_EC
  let (res, eff) = core ctxt args
  -- Produce effect
  case eff of
    Nothing -> return ()
    Just eff -> do
      CQL.executeWrite CQL.ONE (mkInsert tname) (k, s, a + 1, mkVisSet ctxt s, eff)
      idx_EC += 1
  return res

printCtxt :: Storable a => Table -> Key -> (Ctxt a -> Ctxt a) -> EC ()
printCtxt tname k f = do
  rows <- CQL.executeRows CQL.ONE (mkRead tname) k
  let ctxt = mkCtxt rows
  liftIO $ print $ f ctxt

runEC :: CQL.Pool -> EC a -> IO a
runEC pool ec = do
  sess <- liftIO randomIO
  let ecst = ECState sess (0::Index)
  let cas = evalStateT ec ecst
  CQL.runCas pool cas
