{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

module FusionTicketTxns (
  newPlacemap,

  newEvent,
  deleteEvent1,

  newOrder,
  
  reserveSeats,
  reserveSingleSeat,

  checkOut
) where

import Quelea.ClientMonad
import Quelea.TH (checkTxn)

import FusionTicketDefs
import FusionTicketCtrts

import Control.Monad
import Control.Monad.Trans (liftIO)
import System.Random (randomIO)
import Control.Applicative ((<$>))
import Data.Maybe (fromJust)
import Control.Monad (foldM, when)
import Control.Exception.Base
import Data.Time
import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import System.IO (hFlush, stdout)

[newPlacemapTxnCtrtA,
  newEventTxnCtrtA,
  deleteEventTxnCtrtA,
  newOrderTxnCtrtA,
  checkoutTxnCtrtA,
  reserveSeatsTxnCtrtA,
  reserveSingleSeatTxnCtrtA] =
   $(do
      t1 <- runIO getCurrentTime
      a1 <- checkTxn "newPlacemapTxn" newPlacemapTxnCtrt
      a2 <- checkTxn "newEventTxn" newEventTxnCtrt
      a3 <- checkTxn "deleteEventTxn" deleteEventTxnCtrt
      a4 <- checkTxn "newOrderTxn" newOrderTxnCtrt
      a5 <- checkTxn "checkoutTxn" checkoutTxnCtrt
      a6 <- checkTxn "reserveSeatsTxn" reserveSeatsTxnCtrt
      a7 <- checkTxn "reserveSingleSeatTxn" reserveSingleSeatTxnCtrt
      le <- return $ (ListE::[Exp] -> Exp) [a1,a2,a3,a4,a5,a6,a7]
      t2 <- runIO getCurrentTime
      _ <- runIO $ putStrLn $ "----------------------------------------------------------"
      _ <- runIO $ putStrLn $ "Classification of transaction contracts completed in "++
                (show $ diffUTCTime t2 t1)++"."
      _ <- runIO $ putStrLn $ "----------------------------------------------------------"
      _ <- runIO $ hFlush stdout
      return le)

newPlacemap :: Int{-Seating Capacity-} -> CSN (PlacemapID, [SeatID])
newPlacemap capacity = 
	atomically (newPlacemapTxnCtrtA) $ do
		pmid <- liftIO $ PlacemapID <$> randomIO
		liftIO $ putStrLn $ "Creating new placemap " ++ show pmid
		r::() <- invoke (mkKey pmid) CreatePlacemap ((), ())
		liftIO $ putStrLn $ "Created new placemap " ++ show pmid
		seatids <- replicateM (capacity) $ do 
			seatid <- liftIO $ SeatID <$> randomIO
			r::() <- invoke (mkKey seatid) CreateSeat (pmid)
			return seatid
		r::() <- invoke (mkKey pmid) AddSeatsToPlacemap (seatids)
		return $ (pmid, seatids)

newEvent :: PlacemapID->String{-name-}->String{-url-}->UTCTime{-time-}->Int{-limit-}->Int{-price-}->Int{-discount-}->CSN EventID
newEvent pmid name url time limit price discount = do
	resOp::(Maybe EventID, Maybe [SeatID], Bool{-deleted?-}) <- invoke (mkKey pmid) ShowPlacemap ()
	case resOp of
		(Just eid, Just seatids, status) -> do
			if status then 
				atomically (newEventTxnCtrtA) $ do
					eid <- liftIO $ EventID <$> randomIO
					r::() <- invoke (mkKey eid) CreateEvent (pmid,name,url,time,limit)
					r::() <- invoke (mkKey pmid) UpdateEventId (eid)
					did <- liftIO $ DiscountID <$> randomIO
					r::() <- invoke (mkKey did) CreateDiscount (eid,discount) 
					mapM (updateSeatTable eid did) seatids
					return eid
			else error $ "Placemap" ++ (show pmid) ++  "not available"
		otherwise -> error $ "Error creating new event"
	where 
		updateSeatTable :: EventID -> DiscountID -> SeatID -> CSN ()
		updateSeatTable eid did seatId = do
			r::() <- invoke (mkKey seatId) UpdateEventIdAndPriceInSeat (eid,price)
			r::() <- invoke (mkKey seatId) UpdateDiscountInSeat did
			return ()

deleteEvent1 :: EventID -> CSN ()
deleteEvent1 eid = do 
	x :: Maybe (Maybe PlacemapID, Maybe String, Maybe String, Maybe UTCTime, Bool {-Is Selling?-}, Maybe Int {-Number of tickets left-}) <- invoke (mkKey eid) ShowEvent ()
	case x of
	    	Just (Just pmid, Just name, Just url, Just time, False, Just count) -> atomically (deleteEventTxnCtrtA) $ do
	    		resOp :: (Maybe EventID, Maybe [SeatID], Bool{-deleted?-}) <- invoke (mkKey pmid) ShowPlacemap ()
	    		case resOp of 
	    			(Just eid, Just seats, deleted) -> do
		    			if (not deleted) then do
				    		orderids <- mapM deleteSeat seats
				    		mapM deleteOrder orderids
				    		r::() <- invoke (mkKey pmid) DeletePlacemap ()
				    		r::() <- invoke (mkKey eid) DeleteEvent ()
				    		return ()
				    	else error $ "Already deleted event" ++ show eid
				otherwise -> error $ "Error deleting event" ++ show eid
		otherwise -> error $ "Error deleting event" ++ show eid
	where
		deleteSeat :: SeatID -> CSN OrderID
		deleteSeat seatid = do
			resOp :: (Bool {-Status-}, Maybe PlacemapID, Maybe EventID, Maybe DiscountID, Maybe OrderID, Maybe Int{-Price-}) <- invoke (mkKey seatid) ShowSeat ()
			case resOp of
				(status, Just pmid, Just eid, Just did, Just oid, Just price) -> do
					r::() <- invoke (mkKey seatid) Delete ()
					return oid
				otherwise -> error $ "Error deleting seat" ++ show seatid
		deleteOrder :: OrderID -> CSN ()
		deleteOrder oid = do 
			(x) :: Maybe (Maybe CartID, Maybe EventID, Maybe [SeatID], Bool) <- invoke (mkKey oid) GetOrderSummary ()
			case x of
				Just (Just cartId, Just eid, Just seatids, True) -> do
					r::() <- invoke (mkKey oid) CancelOrder ()
					r::() <- invoke (mkKey cartId) RemoveOrdersFromCart oid
					return ()
				otherwise -> return ()

newOrder :: CartID -> EventID -> [SeatID] -> CSN OrderID
newOrder cid eid seatids = do
	(x) :: Maybe (Maybe PlacemapID, Maybe String, Maybe String, Maybe UTCTime, Bool, Maybe Int) <- invoke (mkKey eid) ShowEvent ()
	case x of
		Just (Just pmid, Just name, Just url, Just time, True, Just count) -> atomically (newOrderTxnCtrtA) $ do
			oid <- liftIO $ OrderID <$> randomIO
			mapM checkSeat seatids  
			r::() <- invoke (mkKey oid) CreateOrder (cid,eid,seatids)
			return oid
		Just (Just pmid, Just name, Just url, Just time, False, Just count) -> error "Event not selling :("
		otherwise -> error "Event not available"
	where
		checkSeat :: SeatID -> CSN ()
		checkSeat seatid = do
			(status, _, _, _, _, _) :: (Bool, Maybe PlacemapID, Maybe EventID, Maybe DiscountID, Maybe OrderID, Maybe Int) <- invoke (mkKey seatid) ShowSeat ()
			if status then return ()
			else error $ "Seat" ++ (show seatid) ++ "not available"

reserveSingleSeat :: OrderID -> SeatID -> CSN ()
reserveSingleSeat orderId seatId = atomically (reserveSingleSeatTxnCtrtA) $ do
	(success) :: Bool <- invoke (mkKey seatId) Reserve orderId
	if success then do
		(x) :: Maybe (Maybe OrderID, Maybe EventID, Maybe [SeatID], Bool) <- invoke (mkKey orderId) GetOrderSummary ()
		case x of 
			Just(_,Just eid,_,True) -> do
				resOp::(Bool) <- invoke (mkKey eid) Sell (1::Int)
				if (not resOp) then do
					error $ "Error reserving seat" ++ show seatId
				else return ()
			otherwise -> error $ "Error reserving seat" ++ show seatId
	else error $ "Error reserving seat" ++ show seatId

reserveSeats :: [SeatID] -> OrderID -> CSN ()
reserveSeats seatIds cartId = atomically (reserveSeatsTxnCtrtA) $ do
	mapM (reserveSingleSeat cartId) seatIds
	return ()

checkOut :: CartID -> CSN Int
checkOut cartId = atomically (checkoutTxnCtrtA) $ do
	(orderids) :: [OrderID] <- invoke (mkKey cartId) GetCartSummary ()
	(ordercosts) <- mapM processOrder orderids
	let val = foldl (+) 0 ordercosts
	liftIO $ putStrLn $ "Pay "++(show val)++" ..."
	return val
	where
		seatCost :: SeatID -> CSN Int
		seatCost seatid = do
			resOp :: (Bool, Maybe PlacemapID, Maybe EventID, Maybe DiscountID, Maybe OrderID, Maybe Int) <- invoke (mkKey seatid) ShowSeat ()
			case resOp of 
				(status, Just pmid, Just eid, did, Just cid, Just price) -> do
					(x) :: Maybe (EventID, Int)<- invoke (mkKey did) ShowDiscount ()
					case x of
						Just (eid, val) -> do
							let price = price - val
							return price
				otherwise -> error $ "Error processing seat" ++ show seatid
		getTotalCost :: [SeatID] -> CSN Int {-Cost of order-}
		getTotalCost seatids = do
			cost_of_seats <- mapM seatCost seatids
			let val = foldl (+) 0 cost_of_seats
			return val
		processOrder :: OrderID -> CSN Int {-Cost of order-}
		processOrder orderid = do
			(x) :: Maybe (Maybe OrderID, Maybe EventID, Maybe [SeatID], Bool) <- invoke (mkKey orderid) GetOrderSummary ()
			case x of
				Just (Just orderId, Just eid, Just seatids, True) -> do
					() <- reserveSeats seatids orderId
					total <- getTotalCost seatids
					return total
				otherwise -> error $ "Error processing order" ++ show orderid