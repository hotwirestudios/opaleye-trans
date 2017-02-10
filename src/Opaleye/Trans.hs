{-# LANGUAGE CPP                        #-}
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

    , QueryArrBase(..)

    , runTransaction
    , runTransactionExcept
    , runReadOnlyTransaction
    , runReadOnlyTransactionExcept

    , LockTable(..)
    , GetSearchPath(..)
    , SetLocalSearchPath(..)
    , ReadDBBase(..)
    , UnsafeReadDB(..)
    , WriteDB(..)
    , UnsafeWriteDB(..)

    , -- * Opaleye
      module Export
    ) where

import           Control.Arrow                          (Arrow)
import qualified Control.Exception                      as E
import           Control.Monad                          ((>=>))
import           Control.Monad.Base                     (MonadBase, liftBase)
import           Control.Monad.Except                   (ExceptT, mapExceptT,
                                                         runExceptT)
#if MIN_VERSION_base(4, 9, 0)
import           Control.Monad.Fail                     (MonadFail (fail))
#endif
import           Control.Monad.IO.Class                 (MonadIO, liftIO)
import           Control.Monad.Reader                   (MonadReader,
                                                         ReaderT (..), ask)
import           Control.Monad.Trans                    (MonadTrans (..))
import           Data.Functor                           (void, ($>))
import           Data.Int                               (Int64)
import           Data.List                              (intercalate)
import           Data.Maybe                             (listToMaybe)
import           Data.Profunctor.Product                (ProductProfunctor)
import           Data.Profunctor.Product.Default        (Default)
import           Data.String                            (fromString)
import           Data.Text                              (Text, splitOn, unpack)
import qualified Database.PostgreSQL.Simple             as PSQL
import qualified Database.PostgreSQL.Simple.Transaction as PSQL
import qualified Opaleye                                as OE
import           Opaleye.Internal.Binary                (Binaryspec)
import           Opaleye.Internal.Join                  (NullMaker)
import           Opaleye.Internal.Unpackspec            (Unpackspec)
import           Opaleye.RunQuery                       (QueryRunner)
import           Opaleye.Types
import           System.IO.Error                        (IOError)

import           Opaleye                                as Export hiding
                                                                   (aggregate,
                                                                   aggregateOrdered,
                                                                   countRows,
                                                                   except,
                                                                   exceptAll,
                                                                   intersect,
                                                                   intersectAll,
                                                                   keepWhen,
                                                                   leftJoin,
                                                                   limit,
                                                                   offset,
                                                                   orderBy,
                                                                   restrict,
                                                                   runDelete,
                                                                   runInsert,
                                                                   runInsertMany,
                                                                   runInsertManyReturning,
                                                                   runInsertReturning,
                                                                   runQuery,
                                                                   runUpdate,
                                                                   runUpdateReturning,
                                                                   union,
                                                                   unionAll)

class Monad m => LockTable m where
    lockTable :: LockMode -> [TableName] -> m ()
instance LockTable (Transaction ReadOnly) where
    lockTable = lockTable'
instance LockTable (Transaction ReadWrite) where
    lockTable = lockTable'

class Monad m => GetSearchPath m where
    getSearchPath :: m [Schema]
instance GetSearchPath (Transaction ReadOnly) where
    getSearchPath = getSearchPath'
instance GetSearchPath (Transaction ReadWrite) where
    getSearchPath = getSearchPath'

class Monad m => SetLocalSearchPath m where
    setLocalSearchPath :: [Schema] -> m ()
instance SetLocalSearchPath (Transaction ReadOnly) where
    setLocalSearchPath = setLocalSearchPath'
instance SetLocalSearchPath (Transaction ReadWrite) where
    setLocalSearchPath = toReadWrite . setLocalSearchPath'

class Monad m => ReadDBBase query m where
    runQuery :: Default QueryRunner readerColumns haskells => query () readerColumns -> m [haskells]
    runQueryFirst :: Default QueryRunner readerColumns haskells => query () readerColumns -> m (Maybe haskells)
instance ReadDBBase QueryArr (Transaction ReadOnly) where
    runQuery = runQuery'
    runQueryFirst = runQueryFirst'
instance ReadDBBase QueryArr (Transaction ReadWrite) where
    runQuery = toReadWrite . runQuery'
    runQueryFirst = toReadWrite . runQueryFirst'

class Monad m => UnsafeReadDB m where
    unsafeRunReadOnlyQuery :: (PSQL.Connection -> IO a) -> m a
instance UnsafeReadDB (Transaction ReadOnly) where
    unsafeRunReadOnlyQuery = unsafeWithConnectionIO
instance UnsafeReadDB (Transaction ReadWrite) where
    unsafeRunReadOnlyQuery = unsafeWithConnectionIO

class Monad m => WriteDB m where
    runInsert :: Table writerColumns readerColumns -> writerColumns -> m Int64
    runInsertMany :: Table writerColumns readerColumns -> [writerColumns] -> m Int64
    runInsertReturning :: Default QueryRunner returned haskells => Table writerColumns readerColumns -> writerColumns -> (readerColumns -> returned) -> m [haskells]
    runInsertManyReturning :: Default QueryRunner returned haskells => Table writerColumns readerColumns -> [writerColumns] -> (readerColumns -> returned) -> m [haskells]
    runUpdate :: Table writerColumns readerColumns -> (readerColumns -> writerColumns) -> (readerColumns -> Column PGBool) -> m Int64
    runUpdateReturning :: Default QueryRunner returned haskells => Table writerColumns readerColumns -> (readerColumns -> writerColumns) -> (readerColumns -> Column PGBool) -> (readerColumns -> returned) -> m [haskells]
    runDelete :: Table writerColumns readerColumns -> (readerColumns -> Column PGBool) -> m Int64
instance WriteDB (Transaction ReadWrite) where
    runInsert = runInsert'
    runInsertMany = runInsertMany'
    runInsertReturning = runInsertReturning'
    runInsertManyReturning = runInsertManyReturning'
    runUpdate = runUpdate'
    runUpdateReturning = runUpdateReturning'
    runDelete = runDelete'

class Monad m => UnsafeWriteDB m where
    unsafeRunQuery :: (PSQL.Connection -> IO a) -> m a
instance UnsafeWriteDB (Transaction ReadWrite) where
    unsafeRunQuery = unsafeWithConnectionIO

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
#if MIN_VERSION_base(4, 9, 0)
instance MonadFail (Transaction readWriteMode) where
    fail = error
#endif

data ReadOnly
data ReadWrite

toReadWrite :: Transaction readWriteMode a -> Transaction ReadWrite a
toReadWrite (Transaction t) = Transaction t

class (Arrow query, ProductProfunctor query) => QueryArrBase query where
    restrict :: query (Column PGBool) ()
    keepWhen :: (a -> Column PGBool) -> query a a

    orderBy :: Order a -> query () a -> query () a
    limit :: Int -> query () a -> query () a
    offset :: Int -> query () a -> query () a

    leftJoin :: (Default Unpackspec columnsA columnsA, Default Unpackspec columnsB columnsB, Default NullMaker columnsB nullableColumnsB) => query () columnsA -> query () columnsB -> ((columnsA, columnsB) -> Column PGBool) -> query () (columnsA, nullableColumnsB)

    countRows :: query () a -> query () (Column PGInt8)
    aggregate :: Aggregator a b -> query () a -> query () b
    aggregateOrdered :: Order a -> Aggregator a b -> query () a -> query () b

    unionAll :: Default Binaryspec columns columns => query () columns -> query () columns -> query () columns
    union :: Default Binaryspec columns columns => query () columns -> query () columns -> query () columns

    intersectAll :: Default Binaryspec columns columns => query () columns -> query () columns -> query () columns
    intersect :: Default Binaryspec columns columns => query () columns -> query () columns -> query () columns

    exceptAll :: Default Binaryspec columns columns => query () columns -> query () columns -> query () columns
    except :: Default Binaryspec columns columns => query () columns -> query () columns -> query () columns

instance QueryArrBase OE.QueryArr where
    restrict = OE.restrict
    keepWhen = OE.keepWhen

    orderBy = OE.orderBy
    limit = OE.limit
    offset = OE.offset

    leftJoin = OE.leftJoin

    countRows = OE.countRows
    aggregate = OE.aggregate
    aggregateOrdered = OE.aggregateOrdered

    unionAll = OE.unionAll
    union = OE.union

    intersectAll = OE.intersectAll
    intersect = OE.intersect

    exceptAll = OE.exceptAll
    except = OE.except

-- | Run a postgresql read/write transaction in the 'OpaleyeT' monad
runTransaction :: MonadIO m => Transaction ReadWrite a -> OpaleyeT m a
runTransaction = runTransactionExcept . lift >=> \(Right r) -> pure r

-- | Run a postgresql read/write transaction in the 'OpaleyeT' monad and rollback when an error is thrown in ExceptT
runTransactionExcept :: MonadIO m => ExceptT e (Transaction ReadWrite) a -> OpaleyeT m (Either e a)
runTransactionExcept = unsafeRunTransactionExcept PSQL.ReadWrite

-- | Run a postgresql read-only transaction in the 'OpaleyeT' monad
runReadOnlyTransaction :: MonadIO m => Transaction ReadOnly a -> OpaleyeT m a
runReadOnlyTransaction = runReadOnlyTransactionExcept . lift >=> \(Right r) -> pure r

-- | Run a postgresql read-only transaction in the 'OpaleyeT' monad and rollback when an error is thrown in ExceptT
runReadOnlyTransactionExcept :: MonadIO m => ExceptT e (Transaction ReadOnly) a -> OpaleyeT m (Either e a)
runReadOnlyTransactionExcept = unsafeRunTransactionExcept PSQL.ReadOnly

unsafeRunTransactionExcept :: MonadIO m => PSQL.ReadWriteMode -> ExceptT e (Transaction readWriteMode) a -> OpaleyeT m (Either e a)
unsafeRunTransactionExcept readWriteMode t = unsafeWithConnection $ \env -> unsafeRunTransactionExcept' env readWriteMode t

unsafeRunTransactionExcept' :: Env -> PSQL.ReadWriteMode -> ExceptT e (Transaction readWriteMode) a -> IO (Either e a)
unsafeRunTransactionExcept' (conn, Transaction beforeAction) readWriteMode t =
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
runQuery' :: Default QueryRunner readerColumns haskells => Query readerColumns -> Transaction ReadOnly [haskells]
runQuery' q = unsafeWithConnectionIO (`OE.runQuery` q)

-- | Retrieve the first result from a 'Query'. Similar to @listToMaybe <$> runQuery@.
runQueryFirst' :: Default QueryRunner readerColumns haskells => Query readerColumns -> Transaction ReadOnly (Maybe haskells)
runQueryFirst' q = listToMaybe <$> runQuery' q

-- | Insert into a 'Table'. See 'runInsert'.
runInsert' :: Table writerColumns readerColumns -> writerColumns -> Transaction ReadWrite Int64
runInsert' table columns = unsafeWithConnectionIO (\c -> OE.runInsertMany c table [columns])

-- | Insert many records into a 'Table'. See 'runInsertMany'.
runInsertMany' :: Table writerColumns readerColumns -> [writerColumns] -> Transaction ReadWrite Int64
runInsertMany' table columns = unsafeWithConnectionIO (\c -> OE.runInsertMany c table columns)

-- | Insert a record into a 'Table' with a return value. See 'runInsertReturning'.
runInsertReturning'
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> writerColumns
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
runInsertReturning' table columns ret = unsafeWithConnectionIO (\c -> OE.runInsertManyReturning c table [columns] ret)

-- | Insert many records into a 'Table' with a return value for each record.
runInsertManyReturning'
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> [writerColumns]
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
runInsertManyReturning' table columns ret = unsafeWithConnectionIO (\c -> OE.runInsertManyReturning c table columns ret)

runUpdate' :: Table writerColumns readerColumns -> (readerColumns -> writerColumns) -> (readerColumns -> Column PGBool) -> Transaction ReadWrite Int64
runUpdate' table updates predicate = unsafeWithConnectionIO (\c -> OE.runUpdate c table updates predicate)

runUpdateReturning'
    :: Default QueryRunner returned haskells
    => Table writerColumns readerColumns
    -> (readerColumns -> writerColumns)
    -> (readerColumns -> Column PGBool)
    -> (readerColumns -> returned)
    -> Transaction ReadWrite [haskells]
runUpdateReturning' table updates predicate ret = unsafeWithConnectionIO (\c -> OE.runUpdateReturning c table updates predicate ret)

runDelete' :: Table writerColumns readerColumns -> (readerColumns -> Column PGBool) -> Transaction ReadWrite Int64
runDelete' table predicate = unsafeWithConnectionIO (\c -> OE.runDelete c table predicate)

-- | With a 'Connection' in a 'Transaction'
-- This isn't exposed so that users can't just drop down to IO
-- in a transaction
unsafeWithConnectionIO :: (PSQL.Connection -> IO a) -> Transaction readWriteMode a
unsafeWithConnectionIO f = Transaction (ReaderT f)

getSearchPath' :: Transaction readWriteMode [Schema]
getSearchPath' = unsafeWithConnectionIO $ \connection -> do
    (searchPath:_) <- (PSQL.fromOnly <$>) <$> PSQL.query_ connection "SHOW search_path" :: IO [Text]
    pure $ Schema <$> splitOn "," searchPath

setLocalSearchPath' :: [Schema] -> Transaction readWriteMode ()
setLocalSearchPath' schemas = do
    let searchPath = intercalate "," (("'"++) . (++"'") . unpack . unSchema <$> schemas)
    unsafeWithConnectionIO $ \c -> void $ PSQL.execute_ c (fromString $ "SET LOCAL search_path TO " ++ searchPath)

lockTable' :: LockMode -> [TableName] -> Transaction readWriteMode ()
lockTable' mode tables = do
    let tables' = intercalate "," $ unpack . unTableName <$> tables
    unsafeWithConnectionIO $ \c -> void $ PSQL.execute_ c (fromString $ "LOCK TABLE " ++ tables' ++ " IN " ++ show mode ++ " MODE")
