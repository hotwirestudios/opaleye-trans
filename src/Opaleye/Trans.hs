{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE UndecidableInstances       #-}

module Opaleye.Trans
    ( OpaleyeT (..)
    , runOpaleyeT
    , runOpaleyeT'

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
    , runInsertManyReturning

    , -- * Updates
      runUpdate
    , runUpdateReturning

    , -- * Deletes
      runDelete

    , -- * Utils
      getSearchPath
    , setLocalSearchPath

    , -- * UNSAFE
      unsafeRunQuery
    , unsafeRunReadOnlyQuery

    , -- * Opaleye
      module Export
    ) where

import           Control.Monad.Base                     (MonadBase, liftBase)
import           Control.Monad.IO.Class                 (MonadIO, liftIO)
import           Control.Monad.Reader                   (MonadReader,
                                                         ReaderT (..), ask)
import           Control.Monad.Trans                    (MonadTrans (..))
import           Data.Functor                           (void)
import           Data.Int                               (Int64)
import           Data.List                              (intercalate)
import           Data.Maybe                             (listToMaybe)
import           Data.Profunctor.Product.Default        (Default)
import           Data.Text                              (Text, splitOn, unpack)
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
import           Opaleye.Types
import           Opaleye.Values                         as Export

-- | The 'Opaleye' monad transformer
newtype OpaleyeT m a = OpaleyeT { unOpaleyeT :: ReaderT Env m a }
    deriving (Functor, Applicative, Monad, MonadTrans, MonadIO, MonadReader Env)

instance MonadBase b m => MonadBase b (OpaleyeT m) where
    liftBase = lift . liftBase

type Env = (PSQL.Connection, Transaction ReadOnly ())

-- | Given a 'Connection', run an 'OpaleyeT'
runOpaleyeT :: PSQL.Connection -> OpaleyeT m a -> m a
runOpaleyeT c = runOpaleyeT' c $ pure ()

runOpaleyeT' :: PSQL.Connection -> Transaction ReadOnly () -> OpaleyeT m a -> m a
runOpaleyeT' c beforeAction = flip runReaderT (c, beforeAction) . unOpaleyeT
-- TODO Handle exceptions

newtype Transaction readWriteMode a = Transaction { unTransaction :: ReaderT PSQL.Connection IO a }
    deriving (Functor, Applicative, Monad, MonadReader PSQL.Connection)

newtype ReadOnly = ReadOnly ReadOnly
newtype ReadWrite = ReadWrite ReadWrite

toReadWrite :: Transaction readWriteMode a -> Transaction ReadWrite a
toReadWrite (Transaction t) = Transaction t

-- | Run a postgresql read/write transaction in the 'OpaleyeT' monad
runTransaction :: MonadIO m => Transaction ReadWrite a -> OpaleyeT m a
runTransaction t = unsafeWithConnection $ \env -> unsafeRunTransaction env PSQL.ReadWrite t

-- | Run a postgresql read-only transaction in the 'OpaleyeT' monad
runReadOnlyTransaction :: MonadIO m => Transaction ReadOnly a -> OpaleyeT m a
runReadOnlyTransaction t = unsafeWithConnection $ \env -> unsafeRunTransaction env PSQL.ReadOnly t

unsafeRunTransaction :: Env  -> PSQL.ReadWriteMode -> Transaction readWriteMode a -> IO a
unsafeRunTransaction (conn, Transaction beforeAction) readWriteMode (Transaction t) =
    PSQL.withTransactionMode
        (PSQL.TransactionMode PSQL.defaultIsolationLevel readWriteMode)
        conn
        (runReaderT beforeAction conn *> runReaderT t conn)

unsafeWithConnection :: MonadIO m => (Env -> IO a) -> OpaleyeT m a
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

-- | Insert many records into a 'Table' with a return value for each record.
runInsertManyReturning
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> [writerColumns]
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
runInsertManyReturning table columns ret = unsafeWithConnectionIO (\c -> OE.runInsertManyReturning c table columns ret)

runUpdate :: Table writerColumns readerColumns -> (readerColumns -> writerColumns) -> (readerColumns -> Column PGBool) -> Transaction ReadWrite Int64
runUpdate table updates predicate = unsafeWithConnectionIO (\c -> OE.runUpdate c table updates predicate)

runUpdateReturning
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> (readerColumns -> writerColumns)
    -> (readerColumns -> Column PGBool)
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
runUpdateReturning table updates predicate ret = unsafeWithConnectionIO (\c -> OE.runUpdateReturning c table updates predicate ret)

runDelete :: Table writerColumns readerColumns -> (readerColumns -> Column PGBool) -> Transaction ReadWrite Int64
runDelete table predicate = unsafeWithConnectionIO (\c -> OE.runDelete c table predicate)

unsafeRunQuery :: (PSQL.Connection -> IO a) -> Transaction ReadWrite a
unsafeRunQuery = unsafeWithConnectionIO

unsafeRunReadOnlyQuery :: (PSQL.Connection -> IO a) -> Transaction ReadOnly a
unsafeRunReadOnlyQuery = unsafeWithConnectionIO

-- | With a 'Connection' in a 'Transaction'
-- This isn't exposed so that users can't just drop down to IO
-- in a transaction
unsafeWithConnectionIO :: (PSQL.Connection -> IO a) -> Transaction readWriteMode a
unsafeWithConnectionIO f = Transaction (ReaderT f)

getSearchPath :: Transaction ReadWrite [Schema]
getSearchPath = unsafeRunQuery $ \connection -> do
    (searchPath:_) <- (PSQL.fromOnly <$>) <$> PSQL.query_ connection "SHOW search_path" :: IO [Text]
    pure $ Schema <$> splitOn "," searchPath

setLocalSearchPath :: [Schema] -> Transaction ReadOnly ()
setLocalSearchPath schemas = do
    let searchPath = intercalate "," (unpack . unSchema <$> schemas)
    unsafeWithConnectionIO $ \c -> void $ PSQL.execute c "SET LOCAL search_path TO ?" (PSQL.Only searchPath)
