{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE UndecidableInstances       #-}

module Opaleye.Trans
    ( OpaleyeT (..)
    , runOpaleyeT

    , -- * Transactions
      Transaction
    , ReadOnly
    , ReadWrite
    , toReadWrite
    , transaction
    , transactionReadOnly

    , -- * Queries
      query
    , queryFirst

    , -- * Inserts
      insert
    , insertMany
    , insertReturning
    , insertReturningFirst
    , insertManyReturning

    , unsafeQuery
    ) where

import           Control.Monad.Base                     (MonadBase, liftBase)
import           Control.Monad.IO.Class                 (MonadIO, liftIO)
import           Control.Monad.Reader                   (MonadReader,
                                                         ReaderT (..), ask)
import           Control.Monad.Trans                    (MonadTrans (..))
import           Data.Int                               (Int64)
import           Data.Maybe                             (listToMaybe)
import           Data.Profunctor.Product.Default        (Default)
import qualified Database.PostgreSQL.Simple             as PSQL
import qualified Database.PostgreSQL.Simple.Transaction as PSQL
import           Opaleye

-- | The 'Opaleye' monad transformer
newtype OpaleyeT m a = OpaleyeT { unOpaleyeT :: ReaderT PSQL.Connection m a }
    deriving (Functor, Applicative, Monad, MonadTrans, MonadIO, MonadReader PSQL.Connection)

instance MonadBase b m => MonadBase b (OpaleyeT m) where
    liftBase = lift . liftBase

-- | Given a 'Connection', run an 'OpaleyeT'
runOpaleyeT :: PSQL.Connection -> OpaleyeT m a -> m a
runOpaleyeT c = flip runReaderT c . unOpaleyeT
-- TODO Handle exceptions

newtype Transaction readWriteMode a = Transaction { unTransaction :: ReaderT PSQL.Connection IO a }
    deriving (Functor, Applicative, Monad, MonadReader PSQL.Connection)

newtype ReadOnly = ReadOnly ReadOnly
newtype ReadWrite = ReadWrite ReadWrite

toReadWrite :: Transaction readWriteMode a -> Transaction ReadWrite a
toReadWrite (Transaction t) = Transaction t

-- | Run a postgresql read/write transaction in the 'OpaleyeT' monad
transaction :: MonadIO m => Transaction ReadWrite a -> OpaleyeT m a
transaction t = unsafeWithConnection $ \conn -> unsafeRunTransaction conn PSQL.ReadWrite t

-- | Run a postgresql read-only transaction in the 'OpaleyeT' monad
transactionReadOnly :: MonadIO m => Transaction ReadOnly a -> OpaleyeT m a
transactionReadOnly t = unsafeWithConnection $ \conn -> unsafeRunTransaction conn PSQL.ReadOnly t

unsafeRunTransaction :: PSQL.Connection -> PSQL.ReadWriteMode -> Transaction readWriteMode a -> IO a
unsafeRunTransaction conn readWriteMode (Transaction t) =
    PSQL.withTransactionMode (PSQL.TransactionMode PSQL.defaultIsolationLevel readWriteMode) conn (runReaderT t conn)

unsafeWithConnection :: MonadIO m => (PSQL.Connection -> IO a) -> OpaleyeT m a
unsafeWithConnection f = liftIO . f =<< ask

-- | Execute a 'Query'. See 'runQuery'.
query :: Default QueryRunner readerColumns haskells => Query readerColumns -> Transaction ReadOnly [haskells]
query q = unsafeWithConnectionIO (`runQuery` q)

-- | Retrieve the first result from a 'Query'. Similar to @listToMaybe <$> runQuery@.
queryFirst :: Default QueryRunner readerColumns haskells => Query readerColumns -> Transaction ReadOnly (Maybe haskells)
queryFirst q = listToMaybe <$> query q

-- | Insert into a 'Table'. See 'runInsert'.
insert :: Table writerColumns readerColumns -> writerColumns -> Transaction ReadWrite Int64
insert table columns = unsafeWithConnectionIO (\c -> runInsert c table columns)

-- | Insert many records into a 'Table'. See 'runInsertMany'.
insertMany :: Table writerColumns readerColumns -> [writerColumns] -> Transaction ReadWrite Int64
insertMany table columns = unsafeWithConnectionIO (\c -> runInsertMany c table columns)

-- | Insert a record into a 'Table' with a return value. See 'runInsertReturning'.
insertReturning
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> writerColumns
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
insertReturning table columns ret = unsafeWithConnectionIO (\c -> runInsertReturning c table columns ret)

-- | Insert a record into a 'Table' with a return value. Retrieve only the first result.
-- Similar to @listToMaybe <$> insertReturning@
insertReturningFirst
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> writerColumns
    -> (readerColumns -> returned)
    -> Transaction ReadWrite (Maybe haskells)
insertReturningFirst table columns ret = listToMaybe <$> insertReturning table columns ret

-- | Insert many records into a 'Table' with a return value for each record.
--
-- Maybe not worth defining. This almost certainly does the wrong thing.
insertManyReturning
    :: (MonadIO m, Default QueryRunner returned haskells)
    => Table writerColumns readerColumns
    -> [writerColumns]
    -> (readerColumns -> returned)
    -> OpaleyeT m [[haskells]]
insertManyReturning table columns ret = transaction $ traverse (\col -> insertReturning table col ret) columns

unsafeQuery :: (PSQL.Connection -> IO a) -> Transaction ReadWrite a
unsafeQuery = unsafeWithConnectionIO

-- | With a 'Connection' in a 'Transaction'
-- This isn't exposed so that users can't just drop down to IO
-- in a transaction
unsafeWithConnectionIO :: (PSQL.Connection -> IO a) -> Transaction readWriteMode a
unsafeWithConnectionIO f = Transaction (ReaderT f)
