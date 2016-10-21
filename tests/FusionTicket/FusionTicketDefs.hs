{-# LANGUAGE ScopedTypeVariables, TemplateHaskell #-}

module FusionTicketDefs (
  EventID(..),
  EventEffect(..),
  PlacemapID(..),
  PlacemapEffect(..),
  DiscountID(..),
  DiscountEffect(..),
  CartID(..),
  CartEffect(..),
  SeatID(..),
  SeatEffect(..),
  OrderID(..),
  OrderEffect(..),
  CartOrderEffect(..),
  Operation(..),
  createTables,
  dropTables,

  createEvent, createEventCtrt,
  stopSales, stopSalesCtrt,
  restartSales, restartSalesCtrt,
  returnSeats, returnSeatsCtrt,
  sell, sellCtrt,
  deleteEvent, deleteEventCtrt,
  showEvent, showEventCtrt,
  createPlacemap, createPlacemapCtrt,
  addSeatsToPlacemap, addSeatsToPlacemapCtrt,
  updateDiscountInSeat, updateDiscountInSeatCtrt,
  updateEventId, updateEventIdCtrt,
  updateEventIdAndPriceInSeat, updateEventIdAndPriceInSeatCtrt,
  deletePlacemap, deletePlacemapCtrt,
  showPlacemap, showPlacemapCtrt,
  createDiscount, createDiscountCtrt,
  deleteDiscount, deleteDiscountCtrt,
  showDiscount, showDiscountCtrt,
  createSeat, createSeatCtrt,
  free, freeCtrt,
  deleteSeat, deleteSeatCtrt,
  reserve, reserveCtrt,
  showSeat, showSeatCtrt,
  addCart, addCartCtrt,
  removeCart, removeCartCtrt,
  createOrder, createOrderCtrt,
  cancelOrder, cancelOrderCtrt,
  updateOrder, updateOrderCtrt,
  getOrderSummary, getOrderSummaryCtrt,
  addOrdersToCart, addOrdersToCartCtrt,
  removeOrdersFromCart, removeOrdersFromCartCtrt,
  getCartSummary, getCartSummaryCtrt
)

where

import Database.Cassandra.CQL
import Data.Serialize as S
import qualified Data.Map as M
import Data.Time.Clock
import Control.Applicative ((<$>))
import Data.Tuple.Select (sel1)
import Data.DeriveTH
import Data.UUID

import Quelea.Types
import Quelea.Contract
import Quelea.TH
import Quelea.DBDriver
import Data.List (groupBy, find)

--------------------------------------------------------------------------------------------

newtype EventID = EventID UUID deriving (Eq, Ord, Show)
newtype SeatID = SeatID UUID deriving (Eq, Ord, Show)
newtype PlacemapID = PlacemapID UUID deriving (Eq, Ord, Show)
newtype DiscountID = DiscountID UUID deriving (Eq, Ord, Show)
newtype OrderID = OrderID UUID deriving (Eq, Ord, Show)
newtype CartID = CartID UUID deriving (Eq, Ord, Show)

-- Placemap table : key = pm_id

data PlacemapEffect = CreatePlacemap_ (Maybe EventID) (Maybe [SeatID])
                    | UpdateEventId_ EventID
                    | DeletePlacemap_ 
                    | AddSeatsToPlacemap_ [SeatID]
                    | ShowPlacemap_

$(derive makeSerialize ''PlacemapID)
$(derive makeSerialize ''PlacemapEffect)

instance CasType PlacemapEffect where
  putCas = put
  getCas = get
  casType _ = CBlob

instance Effectish PlacemapEffect where
  summarize l = 
    let ((a, b, c),_) = showPlacemap l () in
    case c of
      False -> []
      True -> [CreatePlacemap_ a b]

createPlacemap :: [PlacemapEffect] -> (Maybe EventID, Maybe [SeatID]) -> ((), Maybe PlacemapEffect)
createPlacemap _ (eid, seatIds) = ((), Just $ CreatePlacemap_ eid seatIds)

updateEventId :: [PlacemapEffect] -> (EventID) -> ((), Maybe PlacemapEffect)
updateEventId _ eid = ((), Just $ UpdateEventId_ eid)

deletePlacemap :: [PlacemapEffect] -> (EventID) -> ((), Maybe PlacemapEffect)
deletePlacemap _ eid = ((), Just $ DeletePlacemap_)

addSeatsToPlacemap :: [PlacemapEffect] -> ([SeatID]) -> ((), Maybe PlacemapEffect)
addSeatsToPlacemap _ seatIDs = ((), Just $ AddSeatsToPlacemap_ seatIDs)

showPlacemap :: [PlacemapEffect] -> () -> ((Maybe EventID, Maybe [SeatID], Bool{-deleted?-}), Maybe PlacemapEffect)
showPlacemap ctxt _ = 
  let x = foldl acc (Nothing, Nothing, False) ctxt
  in (x, Nothing)
  where
    acc ::  (Maybe EventID, Maybe [SeatID], Bool{-deleted?-}) -> PlacemapEffect -> (Maybe EventID, Maybe [SeatID], Bool{-deleted?-})
    acc (eid, seats, status) resOp = 
      case resOp of
        CreatePlacemap_ eid seatIds -> (eid, seatIds, True)
        UpdateEventId_ eid -> 
	        case status of 
	        	True -> (Just eid, seats, True)
	        	otherwise -> (Nothing, Nothing, False)
        AddSeatsToPlacemap_ seatIds -> 
	        case status of 
	        	True -> (eid, Just seatIds, status)
	        	otherwise -> (Nothing, Nothing, False)
        DeletePlacemap_ -> (Nothing, Nothing, False)
        ShowPlacemap_ -> (eid, seats, status)

-- Event table : key = event_id

data EventEffect = CreateEvent_ PlacemapID String {-name-} String {-url-} UTCTime {-time-} Int {-event_seat_limit-} Bool {-Selling?-}
                 | StopSales_
                 | RestartSales_
                 | Sell_ Int {-number of seats-}
                 | Return_ Int {-number of seats-}
                 | DeleteEvent_
                 | ShowEvent_

$(derive makeSerialize ''EventID)
$(derive makeSerialize ''EventEffect)

instance CasType EventEffect where
  putCas = put
  getCas = get
  casType _ = CBlob

createEvent :: [EventEffect] -> (PlacemapID, String, String, UTCTime, Int) -> ((), Maybe EventEffect)
createEvent _ (pmid, name, url, time, limit) = ((), Just $ CreateEvent_ pmid name url time limit False)

stopSales :: [EventEffect] -> () -> ((), Maybe EventEffect)
stopSales ctxt _ = ((), Just $ StopSales_) 

restartSales :: [EventEffect] -> () -> ((), Maybe EventEffect)
restartSales _ _ = ((), Just $ RestartSales_) 

returnSeats :: [EventEffect] -> Int -> ((), Maybe EventEffect)
returnSeats _ num = ((), Just $ Return_ num)

sell :: [EventEffect] -> Int -> ((Bool), Maybe EventEffect)
sell ctxt num = 
  let (a, _) = showEvent ctxt ()
  in acc a 
  where
    acc :: Maybe (Maybe PlacemapID, Maybe String, Maybe String, Maybe UTCTime, Bool, Maybe Int) -> (Bool, Maybe EventEffect)
    acc a = case a of
              Just (_,_,_, _, True, Just bal) -> (\bal num -> if bal >= num then (True, Just $ Sell_ num) else (False, Nothing)) bal num
              otherwise -> (False, Nothing)
      

deleteEvent :: [EventEffect] -> () -> ((), Maybe EventEffect)
deleteEvent _ _ = ((), Just $ DeleteEvent_)

showEvent :: [EventEffect] -> () -> (Maybe (Maybe PlacemapID, Maybe String, Maybe String, Maybe UTCTime, Bool {-Is Selling?-}, Maybe Int {-Number of tickets left-}), Maybe EventEffect)
showEvent ctxt _ =
  case foldl acc Nothing ctxt of
    Nothing -> (Nothing, Nothing)
    Just x -> (Just x, Nothing)
  where
    acc :: Maybe (Maybe PlacemapID, Maybe String, Maybe String, Maybe UTCTime, Bool, Maybe Int) -> EventEffect -> Maybe (Maybe PlacemapID, Maybe String, Maybe String, Maybe UTCTime, Bool, Maybe Int)
    acc resOp (CreateEvent_ pmid name url time limit status) = Just (Just pmid, Just name, Just url, Just time, status, Just limit)
    acc resOp (DeleteEvent_) = Just (Nothing, Nothing, Nothing, Nothing, False, Nothing)
    acc resOp (StopSales_) = 
          case resOp of
                    Just (Just pmid, Just name, Just url, Just time, status, Just count) -> Just (Just pmid, Just name, Just url, Just time, False, Just count)
                    Just x@(Nothing, Nothing, Nothing, Nothing, False, Nothing) -> Just x
                    Nothing -> Nothing
    acc resOp (RestartSales_) = 
          case resOp of
                    Just (Just pmid, Just name, Just url, Just time, status, Just count) -> Just (Just pmid, Just name, Just url, Just time, True, Just count)
                    Just x@(Nothing, Nothing, Nothing, Nothing, False, Nothing) -> Just x
                    Nothing -> Nothing
    acc resOp (Sell_ num) =
          case resOp of
                    Just (Just pmid, Just name, Just url, Just time, True, Just count) -> Just (Just pmid, Just name, Just url, Just time, True, Just $ count - num)
                    Just x@(Just pmid, Just name, Just url, Just time, False, Just count) -> Just x
                    Just x@(Nothing, Nothing, Nothing, Nothing, False, Nothing) -> Just x
                    Nothing -> Nothing
    acc resOp (Return_ num) =
          case resOp of
                    Just (Just pmid, Just name, Just url, Just time, status, Just count) -> Just (Just pmid, Just name, Just url, Just time, status, Just $ count + num)
                    Just x@(Nothing, Nothing, Nothing, Nothing, False, Nothing) -> Just x
                    Nothing -> Nothing
    acc resOp ShowEvent_ = resOp

instance Effectish EventEffect where
  summarize ctxt =
    case showEvent ctxt () of
      (Nothing, _) -> []
      (Just (Nothing, Nothing, Nothing, Nothing, False, Nothing), _) -> [DeleteEvent_]
      (Just (Just a, Just b, Just c, Just d, True, Just e), _) -> [CreateEvent_ a b c d e True]
      (Just (Just a, Just b, Just c, Just d, False, Just e), _) -> [CreateEvent_ a b c d e False]

-- Discount table : key = discount_id

data DiscountEffect = CreateDiscount_ EventID Int {-value of discount-}
                    | DeleteDiscount_
                    | ShowDiscount_ EventID

$(derive makeSerialize ''DiscountID)
$(derive makeSerialize ''DiscountEffect)

instance CasType DiscountEffect where
  putCas = put
  getCas = get
  casType _ = CBlob

createDiscount :: [DiscountEffect] -> (EventID, Int) -> ((), Maybe DiscountEffect)
createDiscount _ (eid, value) = ((), Just $ CreateDiscount_ eid value)

deleteDiscount :: [DiscountEffect] -> (EventID) -> ((), Maybe DiscountEffect)
deleteDiscount _ eid = ((), Just $ DeleteDiscount_)

showDiscount :: [DiscountEffect] -> () -> (Maybe (EventID, Int), Maybe DiscountEffect)
showDiscount ctxt _ =
  case ctxt of
    [CreateDiscount_ eid value] -> (Just (eid,value), Nothing)
    otherwise -> (Nothing, Nothing)

instance Effectish DiscountEffect where
  summarize ctxt =
    let (summary, _) = showDiscount ctxt () in
    case summary of
      Nothing -> []
      Just (eid,value) -> [CreateDiscount_ eid value]

-- Seat table : key = seat_id

data SeatEffect = CreateSeat_ PlacemapID (Maybe EventID) (Maybe DiscountID) (Maybe OrderID) (Maybe Int){-Price-} Bool{-free-}
                | Reserve_ OrderID
                | Free_
                | Delete_
                | ShowSeat_
                | UpdateDiscountInSeat_ DiscountID
                | UpdateEventIdAndPriceInSeat_ EventID Int

$(derive makeSerialize ''SeatID)
$(derive makeSerialize ''SeatEffect)

instance CasType SeatEffect where
  putCas = put
  getCas = get
  casType _ = CBlob

createSeat :: [SeatEffect] -> (PlacemapID, Maybe EventID, Maybe DiscountID, Maybe OrderID, Maybe Int, Bool) -> ((), Maybe SeatEffect)
createSeat _ (pmid, eid, did, oid, price, status) = ((), Just $ CreateSeat_ pmid eid did oid price status)

reserve :: [SeatEffect] -> (OrderID) -> ((Bool), Maybe SeatEffect)
reserve ctxt orderId = 
  let ((status,_,_,_,_,_),_) = showSeat ctxt ()
  in case status of
    True -> (True, Just $ Reserve_ orderId)
    otherwise -> (False, Nothing)

free :: [SeatEffect] -> () -> ((), Maybe SeatEffect)
free _ _ = ((), Just $ Free_)

deleteSeat :: [SeatEffect] -> () -> ((), Maybe SeatEffect)
deleteSeat _ _ = ((), Just $ Delete_)

updateDiscountInSeat :: [SeatEffect] -> (DiscountID) -> ((), Maybe SeatEffect)
updateDiscountInSeat _ discountId = ((), Just $ UpdateDiscountInSeat_ discountId)

updateEventIdAndPriceInSeat :: [SeatEffect] -> (EventID, Int) -> ((), Maybe SeatEffect)
updateEventIdAndPriceInSeat _ (eventId, newPrice) = ((), Just $ UpdateEventIdAndPriceInSeat_ eventId newPrice)

showSeat :: [SeatEffect] -> () -> ((Bool {-Status-}, Maybe PlacemapID, Maybe EventID, Maybe DiscountID, Maybe OrderID, Maybe Int{-Price-}), Maybe SeatEffect)
showSeat ctxt _ = 
  let resM = foldl (\m@(status, pmid, eid, did, cid, price) e ->
               case e of
                 CreateSeat_ pmid1 eid1 did1 cid1 price1 status1-> (status1, Just pmid1, eid1, did1, cid1, price1)
                 Reserve_ orderId-> (False, pmid, eid, did, Just orderId, price)
                 Free_ -> (True, pmid, eid, did, Nothing, price)
                 UpdateDiscountInSeat_ discountId -> (status, pmid, eid, Just discountId, cid, price)
                 UpdateEventIdAndPriceInSeat_ eventId newPrice-> (status, pmid, Just eventId, did, cid, Just newPrice)
                 Delete_ -> (False, Nothing, Nothing, Nothing, Nothing, Nothing)
                 ShowSeat_ -> m) (False, Nothing, Nothing, Nothing, Nothing, Nothing) ctxt
  in (resM, Nothing)

instance Effectish SeatEffect where
  summarize ctxt =
      let (x, _) =  showSeat ctxt () in
      (\x -> 
        case x of 
          (_,Nothing, _, _, _, _) -> []
          (True, Just pmid, eid, did, cid, price) -> [CreateSeat_ pmid eid did cid price True]
          (False, Just pmid, eid, did, Just cartId, price) -> [CreateSeat_ pmid eid did (Just cartId) price False]
          (False, _, _, _, Nothing, _) -> [Delete_]) x

-- Carts table : Key = cart_id

data CartEffect = AddCart_
                | RemoveCart_

$(derive makeSerialize ''CartID)

$(derive makeSerialize ''CartEffect)

instance Effectish CartEffect where
  summarize [AddCart_,RemoveCart_] = []
  summarize [RemoveCart_,AddCart_] = []
  summarize [AddCart_] = [AddCart_]
  summarize [RemoveCart_] = []
  summarize _ = error "Cart summarization encountered strange case"

instance CasType CartEffect where
  putCas = put
  getCas = get
  casType _ = CBlob

addCart :: [CartEffect] -> () -> ((),Maybe CartEffect)
addCart _ () = ((),Just AddCart_)

removeCart :: [CartEffect] -> () -> ((),Maybe CartEffect)
removeCart _ () = ((), Just AddCart_)

-- Order table : key = order_id

data OrderEffect = CreateOrder_ CartID EventID [SeatID]
           | CancelOrder_ 
           | UpdateOrder_ [SeatID]
           | GetOrderSummary_

instance CasType OrderEffect where
  putCas = put
  getCas = get
  casType _ = CBlob

$(derive makeSerialize ''OrderID)

$(derive makeSerialize ''OrderEffect)

createOrder :: [OrderEffect] -> (CartID, EventID, [SeatID]) -> ((), Maybe OrderEffect)
createOrder _ (cartId, eventId, seatIds) = ((), Just $ CreateOrder_ cartId eventId seatIds)

cancelOrder :: [OrderEffect] -> () -> ((), Maybe OrderEffect)
cancelOrder _ _ = ((), Just $ CancelOrder_)

updateOrder :: [OrderEffect] -> ([SeatID]) -> ((), Maybe OrderEffect)
updateOrder _ seatIds = ((), Just $ UpdateOrder_ seatIds)

getOrderSummary :: [OrderEffect] -> () -> (Maybe (Maybe CartID, Maybe EventID, Maybe [SeatID], Bool), Maybe OrderEffect)
getOrderSummary ctxt _ =
  case foldl acc Nothing ctxt of
    Nothing -> (Nothing, Nothing)
    Just x@(a, b, c, d) -> (Just x, Nothing)
    where
      acc :: Maybe(Maybe CartID, Maybe EventID, Maybe [SeatID], Bool) -> OrderEffect -> Maybe (Maybe CartID, Maybe EventID, Maybe [SeatID], Bool)
      acc _ (CreateOrder_ cartId eventId seatIds) = Just (Just cartId, Just eventId, Just seatIds, True)
      acc _ (CancelOrder_) = Just (Nothing, Nothing, Nothing, False)
      acc resOp (GetOrderSummary_) = resOp
      acc resOp (UpdateOrder_ newSeatIds) = 
        case resOp of
          Just (cartId, eventId, _, True) -> Just (cartId, eventId, Just newSeatIds, True)
          Nothing -> Nothing
          otherwise -> resOp

instance Effectish OrderEffect where
  summarize ctxt = 
    let (a, _) = getOrderSummary ctxt ()
    in case a of
      Just (Just b, Just c, Just d, True) -> [CreateOrder_ b c d]
      Nothing -> []
      otherwise -> [CancelOrder_]

--CartOrder table : key = cart_id

data CartOrderEffect = AddOrdersToCart_ OrderID
                     | RemoveOrdersFromCart_ OrderID
                     | GetCartSummary_

$(derive makeSerialize ''CartOrderEffect)

addOrdersToCart :: [CartOrderEffect] -> (OrderID) -> ((), Maybe CartOrderEffect)
addOrdersToCart _ orderId = ((), Just $ AddOrdersToCart_ orderId)

removeOrdersFromCart :: [CartOrderEffect] -> (OrderID) -> ((), Maybe CartOrderEffect)
removeOrdersFromCart _ orderId = ((), Just $ RemoveOrdersFromCart_ orderId)

getCartSummary :: [CartOrderEffect] -> () -> (([OrderID]), Maybe CartOrderEffect)
getCartSummary ctxt () = 
  let resM = foldl (\m e -> case e of 
                      AddOrdersToCart_ orderID -> M.insert orderID True m
                      RemoveOrdersFromCart_ orderID -> M.insert orderID False m) M.empty ctxt
      orderList = foldl (\acc (orderId, status) -> 
                            case status of
                              True -> orderId:acc
                              otherwise -> acc) [] $ M.toList resM
  in (orderList, Nothing)

instance CasType CartOrderEffect where
  putCas = put
  getCas = get
  casType _ = CBlob

instance Effectish CartOrderEffect where
  summarize ctxt =
    let (orderIds, _) = getCartSummary ctxt () in
    foldl (\acc id -> (AddOrdersToCart_ id) : acc) [] orderIds

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

mkOperations [''EventEffect, ''PlacemapEffect, ''SeatEffect, ''DiscountEffect,
  ''CartEffect, ''OrderEffect, ''CartOrderEffect]
$(derive makeSerialize ''Operation)

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Contracts

trueCtrt :: Contract Operation
trueCtrt x = liftProp $ true

createEventCtrt :: Contract Operation
createEventCtrt = trueCtrt

stopSalesCtrt :: Contract Operation
stopSalesCtrt x = forall_ $ \c -> liftProp $ appRel SameObj x c ⇒ (vis c x ∨ vis x c ∨ appRel SameEff x c)

restartSalesCtrt :: Contract Operation
restartSalesCtrt = trueCtrt

returnSeatsCtrt :: Contract Operation
returnSeatsCtrt = trueCtrt

{-Similar to withdraw operation in bank account example-}
sellCtrt :: Contract Operation
sellCtrt x = forallQ_ [Sell] $ \c -> liftProp $ (vis c x ∨ vis x c ∨ appRel SameEff x c)

deleteEventCtrt :: Contract Operation
deleteEventCtrt = trueCtrt

showEventCtrt :: Contract Operation
showEventCtrt x = forall_ $ \a -> liftProp $ hbo a x ⇒ vis a x

createPlacemapCtrt :: Contract Operation
createPlacemapCtrt = trueCtrt

updateEventIdCtrt :: Contract Operation
updateEventIdCtrt = trueCtrt

addSeatsToPlacemapCtrt :: Contract Operation
addSeatsToPlacemapCtrt = trueCtrt

deletePlacemapCtrt :: Contract Operation
deletePlacemapCtrt = trueCtrt

showPlacemapCtrt :: Contract Operation
showPlacemapCtrt x = forall_ $ \a -> liftProp $ hbo a x ⇒ vis a x

createDiscountCtrt :: Contract Operation
createDiscountCtrt = trueCtrt

deleteDiscountCtrt :: Contract Operation
deleteDiscountCtrt = trueCtrt

showDiscountCtrt :: Contract Operation
showDiscountCtrt x = forall_ $ \a -> liftProp $ hbo a x ⇒ vis a x

createSeatCtrt :: Contract Operation
createSeatCtrt = trueCtrt

freeCtrt :: Contract Operation
freeCtrt = trueCtrt

deleteSeatCtrt :: Contract Operation
deleteSeatCtrt = trueCtrt

reserveCtrt :: Contract Operation
reserveCtrt x = forallQ_ [Reserve] $ \c -> liftProp $ (vis c x ∨ vis x c ∨ appRel SameEff x c)

updateDiscountInSeatCtrt :: Contract Operation
updateDiscountInSeatCtrt = trueCtrt

updateEventIdAndPriceInSeatCtrt :: Contract Operation
updateEventIdAndPriceInSeatCtrt = trueCtrt

showSeatCtrt :: Contract Operation
showSeatCtrt x = forall_ $ \a -> liftProp $ hbo a x ⇒ vis a x

addCartCtrt :: Contract Operation
addCartCtrt = trueCtrt

removeCartCtrt :: Contract Operation
removeCartCtrt = trueCtrt

createOrderCtrt :: Contract Operation
createOrderCtrt = trueCtrt

cancelOrderCtrt :: Contract Operation
cancelOrderCtrt = trueCtrt

updateOrderCtrt :: Contract Operation
updateOrderCtrt = trueCtrt

{-Session consistency for a user-}
getOrderSummaryCtrt :: Contract Operation
getOrderSummaryCtrt x = forallQ_ [CreateOrder, CancelOrder, UpdateOrder] $ \a -> liftProp $ soo a x ⇒ vis a x

addOrdersToCartCtrt :: Contract Operation
addOrdersToCartCtrt = trueCtrt

removeOrdersFromCartCtrt :: Contract Operation
removeOrdersFromCartCtrt = trueCtrt

{-Session consistency for a user-}
getCartSummaryCtrt :: Contract Operation
getCartSummaryCtrt x = forallQ_ [AddOrdersToCart, RemoveOrdersFromCart] $ \a -> liftProp $ soo a x ⇒ vis a x

createTables :: Cas ()
createTables = do
  createTxnTable
  createReservationTable
  createTable "EventEffect"
  createTable "PlacemapEffect"
  createTable "DiscountEffect"
  createTable "SeatEffect"
  createTable "CartEffect"
  createTable "OrderEffect"
  createTable "CartOrderEffect"

dropTables :: Cas ()
dropTables = do
  dropTxnTable
  dropReservationTable
  dropTable "EventEffect"
  dropTable "PlacemapEffect"
  dropTable "DiscountEffect"
  dropTable "SeatEffect"
  dropTable "CartEffect"
  dropTable "OrderEffect"
  dropTable "CartOrderEffect"