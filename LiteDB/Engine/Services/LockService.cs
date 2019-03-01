using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace LiteDB
{
    /// <summary>
    /// Implement a locker service locking datafile to shared/reserved and exclusive access mode
    /// Implement both thread lock and process lock
    /// Shared -> Reserved -> Exclusive => !Reserved => !Shared
    /// Reserved -> Exclusive => !Reserved
    /// [Thread Safe]
    /// </summary>
    public class LockService
    {
        #region Properties + Ctor

        private System.Random _rand;
        private TimeSpan _timeout;
        private IDiskService _disk;
        private CacheService _cache;
        private Logger _log;
        private LockState _state;
        private bool _shared = false;
        private ReaderWriterLockSlim _thread = new ReaderWriterLockSlim (LockRecursionPolicy.NoRecursion);

        internal LockService(IDiskService disk, CacheService cache, TimeSpan timeout, Logger log)
        {
            _disk = disk;
            _cache = cache;
            _log = log;
            _timeout = timeout;
            _state = LockState.Unlocked;
            _rand = new System.Random (26);

            _log.Write (Logger.LOCK, "creating lock service with id {0}", _rand.Next ());
        }

        /// <summary>
        /// Get current datafile lock state
        /// </summary>
        public LockState State { get { return _state; } }

        #endregion

        #region Public Methods

        /// <summary>
        /// Enter in Shared lock mode.
        /// </summary>
        public LockControl Shared()
        {
            var read = this.ThreadRead ();
            var shared = this.LockShared ();

            return new LockControl (() =>
            {
                shared ();
                read ();
            });
        }

        /// <summary>
        /// Enter in Reserved lock mode.
        /// </summary>
        public LockControl Reserved()
        {
            var write = this.ThreadWrite ();
            var reserved = this.LockReserved ();

            return new LockControl (() =>
            {
                reserved ();
                write ();
            });
        }

        /// <summary>
        /// Enter in Exclusive lock mode
        /// </summary>
        public LockControl Exclusive()
        {
            var exclusive = this.LockExclusive ();

            return new LockControl (exclusive);
        }

        #endregion

        #region Process lock control

        /// <summary>
        /// Try enter in shared lock (read) - Call action if request a new lock
        /// [Non ThreadSafe]
        /// </summary>
        private Action LockShared()
        {
            lock (_disk)
            {
                if (_state != LockState.Unlocked) return () => { };

                _disk.Lock (LockState.Shared, _timeout);

                _state = LockState.Shared;
                _shared = true;

                _log.Write (Logger.LOCK, "entered in shared lock mode");

                this.AvoidDirtyRead ();

                return () =>
                {
                    _shared = false;
                    _disk.Unlock (LockState.Shared);
                    _state = LockState.Unlocked;

                    _log.Write (Logger.LOCK, "exited shared lock mode");
                };
            }
        }

        /// <summary>
        /// Try enter in reserved mode (read - single reserved)
        /// [ThreadSafe] (always inside an Write())
        /// </summary>
        private Action LockReserved()
        {
            lock (_disk)
            {
                if (_state == LockState.Reserved) return () => { };

                _disk.Lock (LockState.Reserved, _timeout);

                _state = LockState.Reserved;

                _log.Write (Logger.LOCK, "entered in reserved lock mode");

                // can be a new lock, calls action to notifify
                if (!_shared)
                {
                    this.AvoidDirtyRead ();
                }

                // is new lock only when not came from a shared lock
                return () =>
                {
                    _disk.Unlock (LockState.Reserved);

                    _state = _shared ? LockState.Shared : LockState.Unlocked;

                    _log.Write (Logger.LOCK, "exited reserved lock mode");
                };
            }
        }

        /// <summary>
        /// Try enter in exclusive mode (single write)
        /// [ThreadSafe] - always inside Reserved() -> Write() 
        /// </summary>
        private Action LockExclusive()
        {
            lock (_disk)
            {
                if (_state != LockState.Reserved) throw new InvalidOperationException ("Lock state must be reserved");

                // has a shared lock? unlock first (will keep reserved lock)
                if (_shared)
                {
                    _disk.Unlock (LockState.Shared);
                }

                _disk.Lock (LockState.Exclusive, _timeout);

                _state = LockState.Exclusive;

                _log.Write (Logger.LOCK, "entered in exclusive lock mode");

                return () =>
                {
                    _disk.Unlock (LockState.Exclusive);
                    _state = LockState.Reserved;

                    _log.Write (Logger.LOCK, "exited exclusive lock mode");

                    // if was in a shared lock before exclusive lock, back to shared again (still reserved lock)
                    if (_shared)
                    {
                        _disk.Lock (LockState.Shared, _timeout);

                        _log.Write (Logger.LOCK, "backed to shared mode");
                    }
                };
            }
        }

        #endregion

        #region Thread lock control

        /// <summary>
        /// Start new shared read lock control using timeout
        /// </summary>
        private Action ThreadRead()
        {
            var session = _rand.Next (0, 1000);
            _log.Write (Logger.LOCK, "read session {0}", session);

            // if current thread are in read mode, do nothing
            if (_thread.IsReadLockHeld || _thread.IsWriteLockHeld)
            {
                _log.Write (Logger.LOCK, "{0}: returning because isreadlockheld {1}, iswritelockheld {2}",
                    session, _thread.IsReadLockHeld, _thread.IsWriteLockHeld);
                return () => { };
            }

            _log.Write (Logger.LOCK, "{0}: try entering read lock with timeout {1}", session, _timeout);
            // try enter in read mode
            var res = _thread.TryEnterReadLock (_timeout);
            _log.Write (Logger.LOCK, "{0}: read lock state is {1}", session, res);


            return () =>
            {
                // when dispose, close read mode
                try
                {
                    _log.Write (Logger.LOCK, "{0} exiting read lock", session);
                    _thread.ExitReadLock ();
                }
                catch (SynchronizationLockException)
                {
                    _log.Write (Logger.LOCK, "{0} current thread state is {1}",
                            session, Newtonsoft.Json.JsonConvert.SerializeObject (_thread));
                    throw;
                }
            };
        }

        /// <summary>
        /// Start new exclusive write lock control using timeout
        /// </summary>
        private Action ThreadWrite()
        {
            var session = _rand.Next (0, 1000);
            _log.Write (Logger.LOCK, "write session {0}", session);

            // if current thread is already in write mode, do nothing
            if (_thread.IsWriteLockHeld)
            {
                _log.Write (Logger.LOCK, "{0}: returning because iswritelockheld {1}",
                    session, _thread.IsWriteLockHeld);
                return () => { };
            }

            // if current thread is in read mode, exit read mode first
            if (_thread.IsReadLockHeld)
            {
                _log.Write (Logger.LOCK, "{0} readlock is held, exiting read lock", session);
                _thread.ExitReadLock ();

                _log.Write (Logger.LOCK, "{0}: try entering write lock with timeout {1}", session, _timeout);
                var res = _thread.TryEnterWriteLock (_timeout);
                _log.Write (Logger.LOCK, "{0}: write lock state is {1}", session, res);

                // when dispose write mode, enter again in read mode
                return () =>
                {
                    _log.Write (Logger.LOCK, "{0} exiting write lock", session);
                    _thread.ExitWriteLock ();

                    _log.Write (Logger.LOCK, "{0}: try entering read lock with timeout {1}", session, _timeout);
                    var res2 = _thread.TryEnterReadLock (_timeout);
                    _log.Write (Logger.LOCK, "{0}: read lock state is {1}", session, res2);
                };
            }

            _log.Write (Logger.LOCK, "{0}: try entering write lock with timeout {1}", session, _timeout);
            var res3 = _thread.TryEnterWriteLock (_timeout);
            _log.Write (Logger.LOCK, "{0}: write lock state is {1}", session, res3);
            // try enter in write mode
            if (!res3)
            {
                throw LiteException.LockTimeout (_timeout);
            }

            // and release when dispose
            return () =>
            {
                try
                {
                    _log.Write (Logger.LOCK, "{0} exiting write lock", session);
                    _thread.ExitWriteLock ();
                }
                catch (SynchronizationLockException)
                {
                    _log.Write (Logger.LOCK, "{0} current thread state is {1}",
                            session, Newtonsoft.Json.JsonConvert.SerializeObject (_thread));
                    throw;
                }
            };
        }

        #endregion

        /// <summary>
        /// Test if cache still valid (if datafile was changed by another process reset cache)
        /// [Thread Safe]
        /// </summary>
        private void AvoidDirtyRead()
        {
            // if disk are exclusive don't need check dirty read
            if (_disk.IsExclusive) return;

            _log.Write (Logger.CACHE, "checking disk to avoid dirty read");

            // empty cache? just exit
            if (_cache.CleanUsed == 0) return;

            // get ChangeID from cache
            var header = _cache.GetPage (0) as HeaderPage;
            var changeID = header == null ? 0 : header.ChangeID;

            // and get header from disk
            var disk = BasePage.ReadPage (_disk.ReadPage (0)) as HeaderPage;

            // if header change, clear cache and add new header to cache
            if (disk.ChangeID != changeID)
            {
                _log.Write (Logger.CACHE, "file changed from another process, cleaning all cache pages");

                _cache.ClearPages ();
                _cache.AddPage (disk);
            }
        }
    }
}