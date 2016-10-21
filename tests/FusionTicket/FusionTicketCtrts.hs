{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

module FusionTicketCtrts (
  newPlacemapTxnCtrt,
  newEventTxnCtrt,
  deleteEventTxnCtrt,
  newOrderTxnCtrt,
  checkoutTxnCtrt,
  reserveSeatsTxnCtrt,
  reserveSingleSeatTxnCtrt
) where

import FusionTicketDefs
import Quelea.Contract

newPlacemapTxnCtrt :: Fol Operation
newPlacemapTxnCtrt = liftProp $ true

newEventTxnCtrt :: Fol Operation
newEventTxnCtrt = liftProp $ true

deleteEventTxnCtrt :: Fol Operation
deleteEventTxnCtrt = liftProp $ true {-forallQ6_ [CreateEvent][CreatePlacemap][CreateSeat][CreateOrder]
                               [DeleteEvent][DeletePlacemap, Delete, CancelOrder] 
                               $ \a b c d e f-> liftProp $ 
                               			  trans (SameTxn e f) (SameTxn b c) ∧ trans (Single d)(Single a) ∧ sameObj a e ∧ 
                               			  (sameObj b f ∧ vis a e ⇒ vis b f) ∧ (sameObj c f ∧ vis a e ⇒ vis c f) ∧
                               			  (sameObj d f ∧ vis a e ⇒ vis d f)-}
newOrderTxnCtrt :: Fol Operation
newOrderTxnCtrt = liftProp $ true

checkoutTxnCtrt :: Fol Operation
checkoutTxnCtrt = liftProp $ true

reserveSeatsTxnCtrt :: Fol Operation
reserveSeatsTxnCtrt = liftProp $ true

reserveSingleSeatTxnCtrt :: Fol Operation
reserveSingleSeatTxnCtrt = liftProp $ true