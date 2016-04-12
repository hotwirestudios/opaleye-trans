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
    , runTransaction
    , runReadOnlyTransaction

    , -- * Queries
      runQuery
    , runQueryFirst

    , -- * Inserts
      runInsert
    , runInsertMany
    , runInsertReturning
    , runInsertReturningFirst
    , runInsertReturningFirst'
    , runInsertManyReturning

    , -- * Deletes
      runDelete

    , unsafeRunQuery

    , -- * Opaleye
      module Export
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
import           Opaleye.Aggregate                      as Export
import           Opaleye.Binary                         as Export
import           Opaleye.Column                         as Export
import           Opaleye.Constant                       as Export
import           Opaleye.Distinct                       as Export
import           Opaleye.Join                           as Export
import           Opaleye.Label                          as Export
import qualified Opaleye.Manipulation                   as OE
import           Opaleye.Operators                      as Export
import           Opaleye.Order                          as Export
import           Opaleye.PGTypes                        as Export
import           Opaleye.QueryArr                       as Export
import           Opaleye.RunQuery                       (QueryRunner)
import qualified Opaleye.RunQuery                       as OE
import           Opaleye.Sql                            as Export
import           Opaleye.Table                          as Export
import           Opaleye.Values                         as Export

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
runTransaction :: MonadIO m => Transaction ReadWrite a -> OpaleyeT m a
runTransaction t = unsafeWithConnection $ \conn -> unsafeRunTransaction conn PSQL.ReadWrite t

-- | Run a postgresql read-only transaction in the 'OpaleyeT' monad
runReadOnlyTransaction :: MonadIO m => Transaction ReadOnly a -> OpaleyeT m a
runReadOnlyTransaction t = unsafeWithConnection $ \conn -> unsafeRunTransaction conn PSQL.ReadOnly t

unsafeRunTransaction :: PSQL.Connection -> PSQL.ReadWriteMode -> Transaction readWriteMode a -> IO a
unsafeRunTransaction conn readWriteMode (Transaction t) =
    PSQL.withTransactionMode (PSQL.TransactionMode PSQL.defaultIsolationLevel readWriteMode) conn (runReaderT t conn)

unsafeWithConnection :: MonadIO m => (PSQL.Connection -> IO a) -> OpaleyeT m a
unsafeWithConnection f = liftIO . f =<< ask

-- | Execute a 'Query'. See 'runQuery'.
runQuery :: Default QueryRunner readerColumns haskells => Query readerColumns -> Transaction ReadOnly [haskells]
runQuery q = unsafeWithConnectionIO (`OE.runQuery` q)

-- | Retrieve the first result from a 'Query'. Similar to @listToMaybe <$> runQuery@.
runQueryFirst :: Default QueryRunner readerColumns haskells => Query readerColumns -> Transaction ReadOnly (Maybe haskells)
runQueryFirst q = listToMaybe <$> runQuery q

-- | Insert into a 'Table'. See 'runInsert'.
runInsert :: Table writerColumns readerColumns -> writerColumns -> Transaction ReadWrite Int64
runInsert table columns = unsafeWithConnectionIO (\c -> OE.runInsertMany c table [columns])

-- | Insert many records into a 'Table'. See 'runInsertMany'.
runInsertMany :: Table writerColumns readerColumns -> [writerColumns] -> Transaction ReadWrite Int64
runInsertMany table columns = unsafeWithConnectionIO (\c -> OE.runInsertMany c table columns)

-- | Insert a record into a 'Table' with a return value. See 'runInsertReturning'.
runInsertReturning
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> writerColumns
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
runInsertReturning table columns ret = unsafeWithConnectionIO (\c -> OE.runInsertManyReturning c table [columns] ret)

-- | Insert a record into a 'Table' with a return value. Retrieve only the first result.
-- Similar to @listToMaybe <$> insertReturning@
runInsertReturningFirst
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> writerColumns
    -> (readerColumns -> returned)
    -> Transaction ReadWrite (Maybe haskells)
runInsertReturningFirst table columns ret = listToMaybe <$> runInsertReturning table columns ret

runInsertReturningFirst'
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> writerColumns
    -> (readerColumns -> returned)
    -> Transaction ReadWrite haskells
runInsertReturningFirst' table columns ret = head' <$> runInsertReturning table columns ret
    where
        head' [] = error "Return value expected in insertReturningFirst'"
        head' (x:_) = x

-- | Insert many records into a 'Table' with a return value for each record.
runInsertManyReturning
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> [writerColumns]
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
runInsertManyReturning table columns ret = unsafeWithConnectionIO (\c -> OE.runInsertManyReturning c table columns ret)

runDelete :: Table writerColumns readerColumns -> (readerColumns -> Column PGBool) -> Transaction ReadWrite Int64
runDelete table predicate = unsafeWithConnectionIO (\c -> OE.runDelete c table predicate)

unsafeRunQuery :: (PSQL.Connection -> IO a) -> Transaction ReadWrite a
unsafeRunQuery = unsafeWithConnectionIO

-- | With a 'Connection' in a 'Transaction'
-- This isn't exposed so that users can't just drop down to IO
-- in a transaction
unsafeWithConnectionIO :: (PSQL.Connection -> IO a) -> Transaction readWriteMode a
unsafeWithConnectionIO f = Transaction (ReaderT f)
