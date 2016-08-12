{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Opaleye.Types where

import           Data.String (IsString)
import           Data.Text

newtype Schema = Schema { unSchema :: Text } deriving (Eq, Show, IsString)

publicSchema :: Schema
publicSchema = Schema "public"

newtype TableName = TableName { unTableName :: Text } deriving (Eq, Show, IsString)

data LockMode
    = AccessShare
    | RowShare
    | RowExclusive
    | ShareUpdateExclusive
    | Share
    | ShareRowExclusive
    | Exclusive
    | AccessExclusive
    deriving Eq

instance Show LockMode where
    show AccessShare = "ACCESS SHARE"
    show RowShare = "ROW SHARE"
    show RowExclusive = "ROW EXCLUSIVE"
    show ShareUpdateExclusive = "SHARE UPDATE EXCLUSIVE"
    show Share = "SHARE"
    show ShareRowExclusive = "SHARE ROW EXCLUSIVE"
    show Exclusive = "EXCLUSIVE"
    show AccessExclusive  = "ACCESS EXCLUSIVE"
