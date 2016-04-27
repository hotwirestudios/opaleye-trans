{-# LANGUAGE OverloadedStrings #-}

module Opaleye.Types where

import           Data.Text

newtype Schema = Schema { unSchema :: Text }

publicSchema :: Schema
publicSchema = Schema "public"
