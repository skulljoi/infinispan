/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.transaction;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bela
 *         <p/>
 *         Date: May 15, 2003 Time: 4:20:17 PM
 * @since 4.0
 */
public class DummyUserTransaction implements UserTransaction, java.io.Serializable {
   static final Log logger_ = LogFactory.getLog(DummyUserTransaction.class);
   DummyTransactionManager tm_;
   private static final long serialVersionUID = -6568400755677046127L;

   /**
    * List<Synchronization>
    */
   List<Synchronization> l = new ArrayList<Synchronization>();

   public DummyUserTransaction(DummyTransactionManager tm) {
      tm_ = tm;
   }


   /**
    * Starts a new transaction, and associate it with the calling thread.
    *
    * @throws NotSupportedException If the calling thread is already associated with a transaction, and nested
    *                               transactions are not supported.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public void begin() throws NotSupportedException, SystemException {
      tm_.begin();
   }

   /**
    * Attempt to commit this transaction.
    *
    * @throws RollbackException          If the transaction was marked for rollback only, the transaction is rolled back
    *                                    and this exception is thrown.
    * @throws SystemException            If the transaction service fails in an unexpected way.
    * @throws HeuristicMixedException    If a heuristic decision was made and some some parts of the transaction have
    *                                    been committed while other parts have been rolled back.
    * @throws HeuristicRollbackException If a heuristic decision to roll back the transaction was made.
    * @throws SecurityException          If the caller is not allowed to commit this transaction.
    */
   public void commit()
         throws RollbackException, HeuristicMixedException,
                HeuristicRollbackException, SecurityException, SystemException {

      tm_.commit();
   }

   /**
    * Rolls back this transaction.
    *
    * @throws IllegalStateException If the transaction is in a state where it cannot be rolled back. This could be
    *                               because the transaction is no longer active, or because it is in the {@link
    *                               Status#STATUS_PREPARED prepared state}.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public void rollback() throws IllegalStateException, SystemException {
      tm_.rollback();
   }

   /**
    * Mark the transaction so that the only possible outcome is a rollback.
    *
    * @throws IllegalStateException If the transaction is not in an active state.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      tm_.setRollbackOnly();
   }

   /**
    * Get the status of the transaction.
    *
    * @return The status of the transaction. This is one of the {@link Status} constants.
    * @throws SystemException If the transaction service fails in an unexpected way.
    */
   public int getStatus() throws SystemException {
      return tm_.getStatus();
   }

   /**
    * Change the transaction timeout for transactions started by the calling thread with the {@link #begin()} method.
    *
    * @param seconds The new timeout value, in seconds. If this parameter is <code>0</code>, the timeout value is reset
    *                to the default value.
    * @throws SystemException If the transaction service fails in an unexpected way.
    */
   public void setTransactionTimeout(int seconds) throws SystemException {
      throw new SystemException("not supported");
   }

}
