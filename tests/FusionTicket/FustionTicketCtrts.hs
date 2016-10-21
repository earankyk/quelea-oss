{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

module RubisCtrts (
  bidForItemTxnCtrt,
  showMyBidsTxnCtrt,
  cancelBidTxnCtrt,
  openAuctionTxnCtrt,
  showMyAuctionsTxnCtrt,
  concludeAuctionTxnCtrt
) where

import FusionTicketDefs
import Quelea.Contract

newEventTxnCtrt :: Fol Operation
newEventTxnCtrt = liftProp $ true

newOrderTxnCtrt :: Fol Operation
newOrderTxnCtrt = liftProp $ true
