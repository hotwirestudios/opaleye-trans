{-# LANGUAGE OverloadedStrings #-}

module Opaleye.Types where

import           Data.Text

newtype Schema = Schema { unSchema :: Text } deriving (Eq, Show)

publicSchema :: Schema
publicSchema = Schema "public"