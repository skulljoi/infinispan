/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.lucene.locking.LuceneLockFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation that uses Infinispan to store Lucene indices.
 * 
 * Directory locking is assured with
 * {@link org.infinispan.lucene.locking.SharedLuceneLock}
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 * @see org.infinispan.lucene.locking.LuceneLockFactory
 */
// todo add support for ConcurrentMergeSheduler
public class InfinispanDirectory extends Directory {

   private static final Log log = LogFactory.getLog(InfinispanDirectory.class);

   // own flag required if we are not in this same package what org.apache.lucene.store.Directory,
   // access type will be changed in the next Lucene version
   volatile boolean isOpen = true;

   private Cache<CacheKey, Object> cache;
   // indexName is required when one common cache is used
   private String indexName;
   // chunk size used in this directory, static filed not used as we want to have different chunk
   // size per dir
   private int chunkSize;

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName, LockFactory lf, int chunkSize) {
      this.cache = cache;
      this.indexName = indexName;
      this.setLockFactory(lf);
      this.chunkSize = chunkSize;

      // fixme change it to ConcurrentHashMap when [JBMAR-68] bug is fixed
      // Infinispan does not provide efficient API (existing .keySet() is not recommended for
      // production use) to retrieve list of available
      // objects in cache. One entry in cache, store list of lucene file names.
      cache.put(new FileListCacheKey(indexName), Collections.synchronizedMap(new HashMap<String, String>()));
      // register listener which add/remove file names to/from above list
      cache.addListener(new InfinispanCacheEntryListener(indexName));
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName, LockFactory lf) {
      this(cache, indexName, lf, InfinispanIndexIO.DEFAULT_BUFFER_SIZE);
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName, int chunkSize) {
      this(cache, indexName, new LuceneLockFactory(cache, indexName), chunkSize);
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName) {
      this(cache, indexName, new LuceneLockFactory(cache, indexName), InfinispanIndexIO.DEFAULT_BUFFER_SIZE);
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache) {
      this(cache, "");
   }

   /**
    * {@inheritDoc}
    */
   @SuppressWarnings("unchecked")
   public synchronized String[] list() throws IOException {
      checkIsOpen();
      Map<String, String> filesList = (Map<String, String>) cache.get(new FileListCacheKey(indexName));
      return (String[]) filesList.values().toArray(new String[] {});
   }

   /**
    * {@inheritDoc}
    */
   public boolean fileExists(String name) throws IOException {
      checkIsOpen();
      return cache.containsKey(new FileCacheKey(indexName, name));
   }

   /**
    * {@inheritDoc}
    */
   public long fileModified(String name) throws IOException {
      checkIsOpen();
      FileMetadata file = getFile(indexName, name);
      if (file == null) {
         throw new FileNotFoundException(name);
      }
      return file.getLastModified();
   }

   /**
    * {@inheritDoc}
    */
   public void touchFile(String name) throws IOException {
      checkIsOpen();
      FileMetadata file = getFile(indexName, name);
      if (file == null) {
         throw new FileNotFoundException(name);
      }
      file.touch();
   }

   /**
    * {@inheritDoc}
    */
   public void deleteFile(String name) throws IOException {
      checkIsOpen();
      FileCacheKey key = new FileCacheKey(indexName, name);
      // remove main file
      cache.remove(key);
      // and all of its chunks
      int i = 0;
      Object removed;
      ChunkCacheKey chunkKey = new ChunkCacheKey(indexName, name, i);
      do {
         removed = cache.remove(chunkKey);
         chunkKey = new ChunkCacheKey(indexName, name, ++i);
      } while (removed != null);
      if (log.isDebugEnabled()) {
         log.debug("Removed file: {0} from index: {1}", key.getFileName(), indexName);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void renameFile(String from, String to) throws IOException {
      checkIsOpen();
      // rename main file header
      FileMetadata fileFrom = getFile(indexName, from);
      cache.remove(new FileCacheKey(indexName, from));
      cache.put(new FileCacheKey(indexName, to), fileFrom);
      // rename also all chunks
      int i = -1;
      Object ob;
      do {
         ChunkCacheKey chunkKey = new ChunkCacheKey(indexName, from, ++i);
         ob = cache.get(chunkKey);
         if (ob == null) {
            break;
         }
         chunkKey = new ChunkCacheKey(indexName, to, i);
         cache.put(chunkKey, ob);
      } while (true);
      if (log.isTraceEnabled()) {
         log.trace("Renamed file from: {0} to: {1} in index {2}",from, to, indexName);
      }
   }

   /**
    * {@inheritDoc}
    */
   public synchronized long fileLength(String name) throws IOException {
      checkIsOpen();
      final FileMetadata file = getFile(indexName, name);
      if (file == null) {
         throw new FileNotFoundException(name);
      }
      return file.getSize();
   }

   /**
    * {@inheritDoc}
    */
   public synchronized IndexOutput createOutput(String name) throws IOException {
      final FileCacheKey key = new FileCacheKey(indexName, name);
      if (!fileExists(name)) {
         cache.put(key, new FileMetadata());
      }
      return new InfinispanIndexIO.InfinispanIndexOutput(cache, key, chunkSize);
   }

   /**
    * {@inheritDoc}
    */
   public IndexInput openInput(String name) throws IOException {
      final FileCacheKey fileKey = new FileCacheKey(indexName, name);
      return new InfinispanIndexIO.InfinispanIndexInput(cache, fileKey, chunkSize);
   }

   /**
    * {@inheritDoc}
    */
   public void close() throws IOException {
      isOpen = false;
      if (cache != null) {
         cache.stop();
         cache = null;
      }
   }

   private void checkIsOpen() throws AlreadyClosedException {
      if (!isOpen) {
         throw new AlreadyClosedException("this Directory is closed");
      }
   }

   private FileMetadata getFile(String indexName, String fileName) {
      CacheKey key = new FileCacheKey(indexName, fileName);
      return (FileMetadata) cache.get(key);
   }

   @Override
   public String toString() {
      return "InfinispanDirectory{" + "indexName='" + indexName + '\'' + '}';
   }

   public Cache<CacheKey, Object> getCache() {
      return cache;
   }
}
