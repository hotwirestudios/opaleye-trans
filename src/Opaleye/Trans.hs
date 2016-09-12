{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
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
    , toReadWriteExcept
    , runTransaction
    , runTransactionExcept
    , runReadOnlyTransaction
    , runReadOnlyTransactionExcept

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
    , lockTable

    , -- * UNSAFE
      unsafeRunQuery
    , unsafeRunReadOnlyQuery

    , -- * Opaleye
      module Export
    ) where

import qualified Control.Exception                      as E
import           Control.Monad                          ((>=>))
import           Control.Monad.Base                     (MonadBase, liftBase)
import           Control.Monad.Except                   (ExceptT, mapExceptT,
                                                         runExceptT)
import           Control.Monad.IO.Class                 (MonadIO, liftIO)
import           Control.Monad.Reader                   (MonadReader,
                                                         ReaderT (..), ask)
import           Control.Monad.Trans                    (MonadTrans (..))
import           Data.Functor                           (void)
import           Data.Functor                           (($>))
import           Data.Int                               (Int64)
import           Data.List                              (intercalate)
import           Data.Maybe                             (listToMaybe)
import           Data.Profunctor.Product.Default        (Default)
import           Data.String                            (fromString)
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
import           System.IO.Error                        (IOError)

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

newtype Transaction readWriteMode a = Transaction (ReaderT PSQL.Connection IO a)
    deriving (Functor, Applicative, Monad, MonadReader PSQL.Connection)

data ReadOnly
data ReadWrite

toReadWrite :: Transaction readWriteMode a -> Transaction ReadWrite a
toReadWrite (Transaction t) = Transaction t

toReadWriteExcept :: ExceptT e (Transaction readWriteMode) a -> ExceptT e (Transaction ReadWrite) a
toReadWriteExcept = mapExceptT (\(Transaction t) -> Transaction t)

-- | Run a postgresql read/write transaction in the 'OpaleyeT' monad
runTransaction :: MonadIO m => Transaction ReadWrite a -> OpaleyeT m a
runTransaction = runTransactionExcept . lift >=> \(Right r) -> pure r

-- | Run a postgresql read/write transaction in the 'OpaleyeT' monad and rollback when an error is thrown in ExceptT
runTransactionExcept :: MonadIO m => ExceptT e (Transaction ReadWrite) a -> OpaleyeT m (Either e a)
runTransactionExcept t = unsafeWithConnection $ \env -> unsafeRunTransaction env PSQL.ReadWrite t

-- | Run a postgresql read-only transaction in the 'OpaleyeT' monad
runReadOnlyTransaction :: MonadIO m => Transaction ReadOnly a -> OpaleyeT m a
runReadOnlyTransaction = runReadOnlyTransactionExcept . lift >=> \(Right r) -> pure r

-- | Run a postgresql read-only transaction in the 'OpaleyeT' monad and rollback when an error is thrown in ExceptT
runReadOnlyTransactionExcept :: MonadIO m => ExceptT e (Transaction ReadOnly) a -> OpaleyeT m (Either e a)
runReadOnlyTransactionExcept t = unsafeWithConnection $ \env -> unsafeRunTransaction env PSQL.ReadOnly t

unsafeRunTransaction :: Env -> PSQL.ReadWriteMode -> ExceptT e (Transaction readWriteMode) a -> IO (Either e a)
unsafeRunTransaction (conn, Transaction beforeAction) readWriteMode t =
    withTransactionMode (PSQL.TransactionMode PSQL.defaultIsolationLevel readWriteMode) conn $ run (runExceptT t)
    where
        run :: Transaction readWriteMode (Either e a) -> IO (Either e a)
        run (Transaction t) = do
            runReaderT beforeAction conn
            result <- runReaderT t conn
            case result of
                Right result -> PSQL.commit conn $> Right result
                Left e -> PSQL.rollback conn $> Left e

        -- copied from Database.PostgreSQL.Simple.Transaction
        withTransactionMode :: PSQL.TransactionMode -> PSQL.Connection -> IO a -> IO a
        withTransactionMode mode conn act =
            E.mask $ \restore -> do
                PSQL.beginMode mode conn
                r <- restore act `E.onException` rollback_ conn
                --commit conn -- this is the only change
                pure r
            where
                -- | Rollback a transaction, ignoring any @IOErrors@
                rollback_ :: PSQL.Connection -> IO ()
                rollback_ conn = PSQL.rollback conn `E.catch` \(_ :: IOError) -> return ()

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
    let searchPath = intercalate "," (("'"++) . (++"'") . unpack . unSchema <$> schemas)
    unsafeWithConnectionIO $ \c -> void $ PSQL.execute_ c (fromString $ "SET LOCAL search_path TO " ++ searchPath)

lockTable :: LockMode -> [TableName] -> Transaction ReadOnly ()
lockTable mode tables = do
    let tables' = intercalate "," $ unpack . unTableName <$> tables
    unsafeWithConnectionIO $ \c -> void $ PSQL.execute_ c (fromString $ "LOCK TABLE " ++ tables' ++ " IN " ++ show mode ++ " MODE")
