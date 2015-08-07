/*
 *      Copyright (C) 2005-2014 Team XBMC
 *      http://xbmc.org
 *
 *  This Program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2, or (at your option)
 *  any later version.
 *
 *  This Program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with XBMC; see the file COPYING.  If not, see
 *  <http://www.gnu.org/licenses/>.
 *
 */

#include "threads/SystemClock.h"
#include "system.h"
#include "utils/log.h"
#include "threads/SingleLock.h"
#include "utils/TimeUtils.h"
#include "CircularCache.h"

#define MAX_CACHE_AGE 15000 // 15 seconds

using namespace XFILE;

CCircularCache::CCircularCache(size_t front, size_t back, bool bDoubleCache)
 : CCacheStrategy()
 , m_beg1(0)
 , m_end1(0)
 , m_time1(0)
 , m_start1(0)
 , m_beg2(-1)
 , m_end2(-1)
 , m_time2(0)
 , m_start2(m_size / 2)
 , m_readPos(0)
 , m_writePos(0)
 , m_buf(NULL)
 , m_size(front + back)
 , m_size_back(back)
 , m_double_cache(bDoubleCache)
#ifdef TARGET_WINDOWS
 , m_handle(INVALID_HANDLE_VALUE)
#endif
{
}

CCircularCache::~CCircularCache()
{
  Close();
}

int CCircularCache::Open()
{
#ifdef TARGET_WINDOWS
  m_handle = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, m_size, NULL);
  if(m_handle == NULL)
    return CACHE_RC_ERROR;
  m_buf = (uint8_t*)MapViewOfFile(m_handle, FILE_MAP_ALL_ACCESS, 0, 0, 0);
#else
  m_buf = new uint8_t[m_size];
#endif
  if(m_buf == 0)
    return CACHE_RC_ERROR;
  m_beg1 = 0;
  m_end1 = 0;
  m_start1 = 0;
  m_beg2 = -1;
  m_end2 = -1;
  m_start2 = (m_size / 2);
  m_readPos = 0;
  m_writePos = 0;
  return CACHE_RC_OK;
}

void CCircularCache::Close()
{
#ifdef TARGET_WINDOWS
  UnmapViewOfFile(m_buf);
  CloseHandle(m_handle);
  m_handle = INVALID_HANDLE_VALUE;
#else
  delete[] m_buf;
#endif
  m_buf = NULL;
}

size_t CCircularCache::GetMaxWriteSize(const size_t& iRequestSize)
{
  CSingleLock lock(m_sync);

  size_t size1;
  size_t size2;
  if (m_start2 > m_start1)
  {
    size1 = m_start2 - m_start1;
    size2 = m_size - m_start2 + m_start1;
  }
  else
  {
    size1 = m_size - m_start1 + m_start2;
    size2 = m_start1 - m_start2;
  }

  size_t back;
  size_t front;
  size_t pos;
  size_t limit;
  if (m_writePos >= m_beg1 && m_writePos <= m_end1)
  {
    back  = (size_t)(m_writePos - m_beg1);
    front = (size_t)(m_end1 - m_writePos);
    pos   = m_start1 + ((m_start1 + back + front) % size1);

    if ((m_time2 == 0) || (m_time2 + MAX_CACHE_AGE > XbmcThreads::SystemClockMillis()))
    {
      limit = m_size - std::min(back, m_size_back) - front;
    }
    else
    {
      limit = (m_size / 2) - std::min(back, (m_size_back / 2)) - front;
    }
  }
  else
  {
    back  = (size_t)(m_writePos - m_beg2);
    front = (size_t)(m_end2 - m_writePos);
    pos   = m_start2 + ((m_start2 + back + front) % size2);

    limit = m_size - std::min(back, m_size_back) - front;

    if ((m_time1 == 0) || (m_time1 + MAX_CACHE_AGE > XbmcThreads::SystemClockMillis()))
    {
      limit = m_size - std::min(back, m_size_back) - front;
    }
    else
    {
      limit = (m_size / 2) - std::min(back, (m_size_back / 2)) - front;
    }
  }

  // Never return more than limit and size requested by caller
  return std::min(iRequestSize, limit);
}

/**
 * Function will write to m_buf at m_end % m_size location
 * it will write at maximum m_size, but it will only write
 * as much it can without wrapping around in the buffer
 *
 * It will always leave m_size_back of the backbuffer intact
 * but if the back buffer is less than that, that space is
 * usable to write.
 *
 * If back buffer is filled to an larger extent than
 * m_size_back, it will allow it to be overwritten
 * until only m_size_back data remains.
 *
 * The following always apply:
 *  * m_beg <= m_writePos <= m_end
 *  * m_end - m_beg <= m_size
 *
 * Multiple calls may be needed to fill buffer completely.
 */
int CCircularCache::WriteToCache(const char *buf, size_t len)
{
  CSingleLock lock(m_sync);

  size_t size1;
  size_t size2;
  if (m_start2 > m_start1)
  {
    size1 = m_start2 - m_start1;
    size2 = m_size - m_start2 + m_start1;
  }
  else
  {
    size1 = m_size - m_start1 + m_start2;
    size2 = m_start1 - m_start2;
  }

  size_t back;
  size_t front;
  size_t pos;
  size_t limit;
  if (m_writePos >= m_beg1 && m_writePos <= m_end1)
  {
    back  = (size_t)(m_writePos - m_beg1);
    front = (size_t)(m_end1 - m_cur); // FIXME
    pos   = m_start1 + ((back + front) % size1); // FIXME, need to consider m_start2 and limited size

    if ((m_time2 == 0) || (m_time2 + MAX_CACHE_AGE > XbmcThreads::SystemClockMillis()))
    {
      // Other cache expired, make it available for active cache
      limit = m_size - std::min(back, m_size_back) - front;
    }
    else
    {
      // Treat both caches as equals
      limit = (m_size / 2) - std::min(back, (m_size_back / 2)) - front;
    }
  }
  else
  {
    back  = (size_t)(m_writePos - m_beg2);
    front = (size_t)(m_end2 - m_cur); // FIXME
    pos   = m_start2 + ((back + front) % size2); // FIXME, need to consider m_start2 and limited size

    if ((m_time1 == 0) || (m_time1 + MAX_CACHE_AGE > XbmcThreads::SystemClockMillis()))
    {
      // Other cache expired, make it available for active cache
      limit = m_size - std::min(back, m_size_back) - front;
    }
    else
    {
      // Treat both caches as equals
      limit = (m_size / 2) - std::min(back, (m_size_back / 2)) - front;
    }
  }

  // FIXME: Must also limit by the cache border
  const size_t wrap  = m_size - pos;

  // limit by max forward size
  if(len > limit)
    len = limit;

  // limit to wrap point
  if(len > wrap)
    len = wrap;
    
  if(len == 0)
    return 0;

  // write the data
  memcpy(m_buf + pos, buf, len);

  m_writePos += len; // Update write position

  if (m_writePos >= m_beg1 && m_writePos <= m_end1)
  {
    m_end1 += len;

    if ( (size_t) (m_end1 - m_beg1) > size1)
    {
      // drop history in other cache that was overwritten
      size_t overwritten = (m_end1 - m_beg1) - size1;
      m_beg2 += overwritten;
      m_start2 += overwritten % m_size; // FIXME?
    }
    else if (m_end1 - m_beg1 > (int64_t)m_size) // FIXME
    {
      // drop history that was overwritten
      m_beg1 = m_end1 - m_size;
    }
  }
  else
  {
    m_end2 += len;

    if ( (size_t) (m_end2 - m_beg2) > size2)
    {
      // drop history in other cache that was overwritten
      size_t overwritten = (m_end2 - m_beg2) - size2;
      m_beg1 += overwritten;
      m_start1 += overwritten % m_size; // FIXME?
    }
    else if (m_end1 - m_beg1 > (int64_t)m_size) // FIXME
    {
      // drop history that was overwritten
      m_beg1 = m_end1 - m_size; //fixme
    }
  }

  m_written.Set();

  return len;
}

/**
 * Reads data from cache. Will only read up till
 * the buffer wrap point. So multiple calls
 * may be needed to empty the whole cache
 */
int CCircularCache::ReadFromCache(char *buf, size_t len)
{
  CSingleLock lock(m_sync);

  size_t pos;
  size_t front;

  size_t size1;
  size_t size2;
  if (m_start2 > m_start1)
  {
    size1 = m_start2 - m_start1;
    size2 = m_size - m_start2 + m_start1;
  }
  else
  {
    size1 = m_size - m_start1 + m_start2;
    size2 = m_start1 - m_start2;
  }

  if (m_readPos >= m_beg1 && m_readPos <= m_end1)
  {
    pos     = m_start1 + ((m_start1 + back + front) % size1); // FIXME, need to consider m_start2 and limited size
    front   = (size_t)(m_end1 - m_readPos);
    m_time1 = XbmcThreads::SystemClockMillis(); // Update last used time
  }
  else
  {
    pos     = m_start2 + ((m_start2 + back + front) % size2); // FIXME, need to consider m_start2 and limited size
    front   = (size_t)(m_end2 - m_readPos);
    m_time2 = XbmcThreads::SystemClockMillis(); // Update last used time
  }

  size_t avail = std::min(m_size - pos, front); // Limit by wrap point (or front size)

  if(avail == 0)
  {
    if(IsEndOfInput())
      return 0;
    else
      return CACHE_RC_WOULD_BLOCK;
  }

  if(len > avail)
    len = avail;

  if(len == 0)
    return 0;

  memcpy(buf, m_buf + pos, len);
  m_readPos += len;

  m_space.Set();

  return len;
}

/* Wait "millis" milliseconds for "minimum" amount of data to come in.
 * Note that caller needs to make sure there's sufficient space in the forward
 * buffer for "minimum" bytes else we may block the full timeout time
 */
int64_t CCircularCache::WaitForData(unsigned int minimum, unsigned int millis)
{
  CSingleLock lock(m_sync);

  int64_t avail;
  if (m_readPos >= m_beg1 && m_readPos <= m_end1)
    avail = m_end1 - m_readPos;
  else
    avail = m_end2 - m_readPos;

  if (millis == 0 || IsEndOfInput())
    return avail; // FIXME?

  if (minimum > (m_size - m_size_back) / 2) // Take into account 2 active sub caches
    minimum = (m_size - m_size_back) / 2;

  XbmcThreads::EndTime endtime(millis);
  while (!IsEndOfInput() && avail < minimum && !endtime.IsTimePast() )
  {
    lock.Leave();
    m_written.WaitMSec(50); // may miss the deadline. shouldn't be a problem.
    lock.Enter();

    if (m_readPos >= m_beg1 && m_readPos <= m_end1)
      avail = m_end1 - m_readPos;
    else
      avail = m_end2 - m_readPos;
  }

  return avail;
}

int64_t CCircularCache::Seek(int64_t pos)
{
  CSingleLock lock(m_sync);

  // if seek is a bit over what we have, try to wait a few seconds for the data to be available.
  // we try to avoid a (heavy) seek on the source
  if (pos >= m_end1 && pos < m_end1 + 100000)
  {
    /* Make everything in the cache (back & forward) back-cache, to make sure
     * there's sufficient forward space. Increasing it with only 100000 may not be
     * sufficient due to variable filesystem chunksize
     */
    m_readPos = m_end1;
    lock.Leave();
    WaitForData((size_t)(pos - m_readPos), 5000);
    lock.Enter();
  }
  else if (pos >= m_end2 && pos < m_end2 + 100000)
  {
    /* Make everything in the cache (back & forward) back-cache, to make sure
     * there's sufficient forward space. Increasing it with only 100000 may not be
     * sufficient due to variable filesystem chunksize
     */
    m_readPos = m_end2;
    lock.Leave();
    WaitForData((size_t)(pos - m_readPos), 5000);
    lock.Enter();
  }

  if ((pos >= m_beg1 && pos <= m_end1) || (pos >= m_beg2 && pos <= m_end2))
  {
    m_readPos = pos;
    return pos;
  }

  return CACHE_RC_ERROR;
}

void CCircularCache::Reset(int64_t pos, bool clearAnyway)
{
  CSingleLock lock(m_sync);
  if (clearAnyway || !IsCachedPosition(pos))
  {
    // FIXME: cache swap logic, take into account cache age
    if (m_readPos >= m_beg1 && m_readPos <= m_end1)
    {
      // Switch to cache 2
      m_end2 = pos;
      m_beg2 = pos;
    }
    else
    {
      // Switch to cache 1
      m_end1 = pos;
      m_beg1 = pos;
    }
  }

  m_readPos = pos;
}

int64_t CCircularCache::CachedDataEndPosIfSeekTo(int64_t iFilePosition)
{
  if (iFilePosition >= m_beg1 && iFilePosition <= m_end1)
    return m_end1;

  if (iFilePosition >= m_beg2 && iFilePosition <= m_end2)
    return m_end2;

  return iFilePosition;
}

int64_t CCircularCache::CachedDataEndPos()
{
  if (m_readPos >= m_beg1 && m_readPos <= m_end1)
    return m_end1;
  else
    return m_end2;
}

bool CCircularCache::IsCachedPosition(int64_t iFilePosition)
{
  return ((iFilePosition >= m_beg1 && iFilePosition <= m_end1) || (iFilePosition >= m_beg2 && iFilePosition <= m_end2));
}

CCacheStrategy *CCircularCache::CreateNew()
{
  return new CCircularCache(m_size - m_size_back, m_size_back, false);
}
