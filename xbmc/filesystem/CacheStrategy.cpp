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
#include "CacheStrategy.h"
#include "IFile.h"
#include "Util.h"
#include "utils/log.h"
#include "SpecialProtocol.h"
#include "PlatformDefs.h" //for PRIdS, PRId64
#include "URL.h"
#if defined(TARGET_POSIX)
#include "posix/PosixFile.h"
#define CacheLocalFile CPosixFile
#elif defined(TARGET_WINDOWS)
#include "win32/Win32File.h"
#define CacheLocalFile CWin32File
#endif // TARGET_WINDOWS

#include <cassert>
#include <algorithm>

// Define cache age before we consider its data obsolete
#define CACHE_AGE 15000

using namespace XFILE;

CCacheStrategy::CCacheStrategy() : m_bEndOfInput(false)
{
}


CCacheStrategy::~CCacheStrategy()
{
}

void CCacheStrategy::EndOfInput() {
  m_bEndOfInput = true;
}

bool CCacheStrategy::IsEndOfInput()
{
  return m_bEndOfInput;
}

void CCacheStrategy::ClearEndOfInput()
{
  m_bEndOfInput = false;
}

CSimpleFileCache::CSimpleFileCache()
  : m_cacheFileRead(new CacheLocalFile())
  , m_cacheFileWrite(new CacheLocalFile())
  , m_hDataAvailEvent(NULL)
  , m_nStartPosition(0)
  , m_nWritePosition(0)
  , m_nReadPosition(0) {
}

CSimpleFileCache::~CSimpleFileCache()
{
  Close();
  delete m_cacheFileRead;
  delete m_cacheFileWrite;
}

int CSimpleFileCache::Open()
{
  Close();

  m_hDataAvailEvent = new CEvent;

  m_filename = CSpecialProtocol::TranslatePath(CUtil::GetNextFilename("special://temp/filecache%03d.cache", 999));
  if (m_filename.empty())
  {
    CLog::Log(LOGERROR, "%s - Unable to generate a new filename", __FUNCTION__);
    Close();
    return CACHE_RC_ERROR;
  }

  CURL fileURL(m_filename);

  if (!m_cacheFileWrite->OpenForWrite(fileURL, false))
  {
    CLog::LogF(LOGERROR, "failed to create file \"%s\" for writing", m_filename.c_str());
    Close();
    return CACHE_RC_ERROR;
  }

  if (!m_cacheFileRead->Open(fileURL))
  {
    CLog::LogF(LOGERROR, "failed to open file \"%s\" for reading", m_filename.c_str());
    Close();
    return CACHE_RC_ERROR;
  }

  return CACHE_RC_OK;
}

void CSimpleFileCache::Close()
{
  if (m_hDataAvailEvent)
    delete m_hDataAvailEvent;

  m_hDataAvailEvent = NULL;

  m_cacheFileWrite->Close();
  m_cacheFileRead->Close();

  if (!m_filename.empty() && !m_cacheFileRead->Delete(CURL(m_filename)))
    CLog::LogF(LOGWARNING, "failed to delete temporary file \"%s\"", m_filename.c_str());

  m_filename.clear();
}

size_t CSimpleFileCache::GetMaxWriteSize(const size_t& iRequestSize)
{
  return iRequestSize; // Can always write since it's on disk
}

int CSimpleFileCache::WriteToCache(const char *pBuffer, size_t iSize)
{
  size_t written = 0;
  while (iSize > 0)
  {
    const ssize_t lastWritten = m_cacheFileWrite->Write(pBuffer, (iSize > SSIZE_MAX) ? SSIZE_MAX : iSize);
    if (lastWritten <= 0)
    {
      CLog::LogF(LOGERROR, "failed to write to file");
      return CACHE_RC_ERROR;
    }
    m_nWritePosition += lastWritten;
    iSize -= lastWritten;
    written += lastWritten;
  }

  // when reader waits for data it will wait on the event.
  m_hDataAvailEvent->Set();

  return written;
}

int64_t CSimpleFileCache::GetAvailableRead()
{
  return m_nWritePosition - m_nReadPosition;
}

int CSimpleFileCache::ReadFromCache(char *pBuffer, size_t iMaxSize)
{
  int64_t iAvailable = GetAvailableRead();
  if ( iAvailable <= 0 )
    return m_bEndOfInput? 0 : CACHE_RC_WOULD_BLOCK;

  size_t toRead = ((int64_t)iMaxSize > iAvailable) ? (size_t)iAvailable : iMaxSize;

  size_t readBytes = 0;
  while (toRead > 0)
  {
    const ssize_t lastRead = m_cacheFileRead->Read(pBuffer, (toRead > SSIZE_MAX) ? SSIZE_MAX : toRead);
    if (lastRead == 0)
      break;
    if (lastRead < 0)
    {
      CLog::LogF(LOGERROR, "failed to read from file");
      return CACHE_RC_ERROR;
    }
    m_nReadPosition += lastRead;
    toRead -= lastRead;
    readBytes += lastRead;
  }

  if (readBytes > 0)
    m_space.Set();

  return readBytes;
}

int64_t CSimpleFileCache::WaitForData(unsigned int iMinAvail, unsigned int iMillis)
{
  if( iMillis == 0 || IsEndOfInput() )
    return GetAvailableRead();

  XbmcThreads::EndTime endTime(iMillis);
  while (!IsEndOfInput())
  {
    int64_t iAvail = GetAvailableRead();
    if (iAvail >= iMinAvail)
      return iAvail;

    if (!m_hDataAvailEvent->WaitMSec(endTime.MillisLeft()))
      return CACHE_RC_TIMEOUT;
  }
  return GetAvailableRead();
}

int64_t CSimpleFileCache::Seek(int64_t iFilePosition)
{
  int64_t iTarget = iFilePosition - m_nStartPosition;

  if (iTarget < 0)
  {
    CLog::Log(LOGDEBUG,"CSimpleFileCache::Seek, request seek before start of cache.");
    return CACHE_RC_ERROR;
  }

  int64_t nDiff = iTarget - m_nWritePosition;
  if (nDiff > 500000 || (nDiff > 0 && WaitForData((unsigned int)(iTarget - m_nReadPosition), 5000) == CACHE_RC_TIMEOUT))
  {
    CLog::Log(LOGDEBUG,"CSimpleFileCache::Seek - Attempt to seek past read data");
    return CACHE_RC_ERROR;
  }

  m_nReadPosition = m_cacheFileRead->Seek(iTarget, SEEK_SET);
  if (m_nReadPosition != iTarget)
  {
    CLog::LogF(LOGERROR, "can't seek file");
    return CACHE_RC_ERROR;
  }

  m_space.Set();

  return iFilePosition;
}

bool CSimpleFileCache::Reset(int64_t iSourcePosition, bool clearAnyway)
{
  if (!clearAnyway && IsCachedPosition(iSourcePosition))
  {
    m_nReadPosition = m_cacheFileRead->Seek(iSourcePosition - m_nStartPosition, SEEK_SET);
    return false;
  }

  m_nStartPosition = iSourcePosition;
  m_nWritePosition = m_cacheFileWrite->Seek(0, SEEK_SET);
  m_nReadPosition = m_cacheFileRead->Seek(0, SEEK_SET);
  return true;
}

void CSimpleFileCache::EndOfInput()
{
  CCacheStrategy::EndOfInput();
  m_hDataAvailEvent->Set();
}

int64_t CSimpleFileCache::CachedDataEndPosIfSeekTo(int64_t iFilePosition)
{
  if (iFilePosition >= m_nStartPosition && iFilePosition <= m_nStartPosition + m_nWritePosition)
    return m_nStartPosition + m_nWritePosition;
  return iFilePosition;
}

int64_t CSimpleFileCache::CachedDataEndPos()
{
  return m_nStartPosition + m_nWritePosition;
}

int64_t CSimpleFileCache::CachedDataBeginPos()
{
  return m_nStartPosition;
}

bool CSimpleFileCache::IsCachedPosition(int64_t iFilePosition)
{
  return iFilePosition >= m_nStartPosition && iFilePosition <= m_nStartPosition + m_nWritePosition;
}

CCacheStrategy *CSimpleFileCache::CreateNew()
{
  return new CSimpleFileCache();
}


CDoubleCache::CDoubleCache(CCacheStrategy *impl)
{
  assert(NULL != impl);
  m_pCache1 = impl;
  m_pReadCache = impl;
  m_pWriteCache = impl;
  m_iLastCacheTime1 = 0;
  m_iLastCacheTime2 = 0;

  m_pCache2 = m_pCache1->CreateNew();
}

CDoubleCache::~CDoubleCache()
{
  delete m_pCache1;
  delete m_pCache2;
    printf("destruct\n");
}

int CDoubleCache::Open()
{
  printf("Open1\n");
  int iRes = m_pCache1->Open();
  if (iRes == CACHE_RC_OK)
  {
    printf("Open2\n");
    return m_pCache2->Open();
  }

  return iRes;
}

void CDoubleCache::Close()
{
  printf("Close\n");
  m_pCache1->Close();
  m_pCache2->Close();
}

size_t CDoubleCache::GetMaxWriteSize(const size_t& iRequestSize)
{
  size_t iFree = m_pWriteCache->GetMaxWriteSize(iRequestSize);

  if (m_pCache1 == m_pWriteCache)
  {
    // Check cache1 is active, so check cache2 (age)
    if (m_iLastCacheTime2 == 0 || m_iLastCacheTime2 + CACHE_AGE < XbmcThreads::SystemClockMillis())
    {
      return std::min(iFree + m_pCache2->GetMaxWriteSize(iRequestSize), iRequestSize);
    }
  }
  else
  {
    // Check cache2 is active, so check cache1 (age)
    if (m_iLastCacheTime1 == 0 || m_iLastCacheTime1 + CACHE_AGE < XbmcThreads::SystemClockMillis())
    {
      return std::min(iFree + m_pCache1->GetMaxWriteSize(iRequestSize), iRequestSize);
    }

  }

  return iFree;
}

int CDoubleCache::WriteToCache(const char *pBuffer, size_t iSize)
{
  size_t iWritten = m_pWriteCache->WriteToCache(pBuffer, iSize);

  if (iWritten >= 0 && iWritten < iSize) // Full?
  {
    printf("iWritten = %li iSize = %li\n", iWritten, iSize);
    if (m_pCache1 == m_pWriteCache)
    {
      // Cache1 is active, so check cache2 (age)
      if (m_iLastCacheTime2 == 0 || m_iLastCacheTime2 + CACHE_AGE < XbmcThreads::SystemClockMillis())
      {
        printf("Switch to writecache2\n");
        m_pWriteCache = m_pCache2; // Switch to cache 2 for write
        m_pWriteCache->Reset(m_pCache1->CachedDataEndPos() + 1);  // FIXME for EOF
        int iWritten2 = m_pWriteCache->WriteToCache(pBuffer + iWritten, iSize - iWritten);
        if (iWritten2 > 0)
          iWritten += iWritten2;
      }
    }
    else
    {
      // Cache2 is active, so check cache1 (age)
      if (m_iLastCacheTime1 == 0 || m_iLastCacheTime1 + CACHE_AGE < XbmcThreads::SystemClockMillis())
      {
        printf("Switch to writecache1\n");
        m_pWriteCache = m_pCache1; // Switch to cache 1 for write
        m_pWriteCache->Reset(m_pCache2->CachedDataEndPos() + 1);  // FIXME for EOF
        int iWritten2 = m_pWriteCache->WriteToCache(pBuffer + iWritten, iSize - iWritten);
        if (iWritten2 > 0)
          iWritten += iWritten2;
      }
    }
  }

  return iWritten;
}

int CDoubleCache::ReadFromCache(char *pBuffer, size_t iMaxSize)
{
  int iRead = m_pReadCache->ReadFromCache(pBuffer, iMaxSize);

  if (iRead > 0)
  {
    // Got data: Update timestamp for active read cache
    if (m_pReadCache == m_pCache1)
      m_iLastCacheTime1 = XbmcThreads::SystemClockMillis();
    else
      m_iLastCacheTime2 = XbmcThreads::SystemClockMillis();
  }
  printf("Read cache with iRead = %li iMaxSize = %li\n", iRead, iMaxSize);
  if (m_pReadCache == m_pCache1)
    printf("***********current read cache is 1\n");
  else
    printf("***********current read cache is 2\n");

  // FIXME: How about if the caches are empty at this point?
  if (iRead >= 0 && iRead < iMaxSize)
  {
    printf("1 begin = %li end = %li age = %li \n", m_pCache1->CachedDataBeginPos(), m_pCache1->CachedDataEndPos(), m_iLastCacheTime1);
    printf("2 begin = %li end = %li age = %li \n", m_pCache2->CachedDataBeginPos(), m_pCache2->CachedDataEndPos(), m_iLastCacheTime2);
    // Switch to other cache if no data left in current read cache and caches are stacked
    if (m_pReadCache->CachedDataEndPos() == m_pCache2->CachedDataBeginPos() + 1)
    {
      // Read remaining data (if any)
      int iRead2 = m_pCache2->ReadFromCache(pBuffer + iRead, iMaxSize - iRead);
      if (iRead2 > 0)
      {
        printf("Switch to readcache2\n");
        m_pReadCache = m_pCache2;
        m_iLastCacheTime2 = XbmcThreads::SystemClockMillis();
        iRead += iRead2;
      }
    }
    else if (m_pReadCache->CachedDataEndPos() == m_pCache1->CachedDataBeginPos() + 1)
    {
      // Read remaining data (if any)
      int iRead2 = m_pCache1->ReadFromCache(pBuffer + iRead, iMaxSize - iRead);
      if (iRead2 > 0)
      {
        printf("Switch to readcache1\n");
        m_pReadCache = m_pCache1;
        m_iLastCacheTime1 = XbmcThreads::SystemClockMillis();
        iRead += iRead2;
      }
    }
  }

  return iRead;
}

int64_t CDoubleCache::WaitForData(unsigned int iMinAvail, unsigned int iMillis)
{
  if (iMillis == 0)
  {
    // Cached size requested, return total for both caches
    return m_pCache1->WaitForData(iMinAvail, 0) + m_pCache2->WaitForData(iMinAvail, 0);
  }
  printf("waitfordata start\n");
  // FIXME: Need to check the other cache as well and not ask for more than available!
  int64_t res = m_pReadCache->WaitForData(iMinAvail, iMillis);
  printf("waitfordata done\n");
  return res;
}

int64_t CDoubleCache::Seek(int64_t iFilePosition)
{
  // FIXME: Waitfor data is broken?
  if (!m_pCache2->IsCachedPosition(iFilePosition))
  {
    if (m_pCache1->Seek(iFilePosition) == iFilePosition)
    {
      m_pReadCache = m_pCache1;
      return iFilePosition;
    }
  }

  if (!m_pCache1->IsCachedPosition(iFilePosition))
  {
    if (m_pCache2->Seek(iFilePosition) == iFilePosition)
    {
      m_pReadCache = m_pCache2;
      return iFilePosition;
    }
  }

  return CACHE_RC_ERROR; // Request seek event
}

bool CDoubleCache::Reset(int64_t iSourcePosition, bool clearAnyway)
{
  if (!clearAnyway && m_pCache1->IsCachedPosition(iSourcePosition)
      && (!m_pCache2->IsCachedPosition(iSourcePosition)
          || m_pCache1->CachedDataEndPos() >= m_pCache2->CachedDataEndPos()))
  {
    printf("reset keep1\n");
    m_pWriteCache = m_pCache1;
  }
  else
  {
    // Check cache age and use the oldest
    if (m_iLastCacheTime1 == 0 || (m_iLastCacheTime2 != 0 && m_iLastCacheTime1 > m_iLastCacheTime2))
    {
      printf("reset swap to1\n");
      m_pWriteCache = m_pCache1;
    }
    else
    {
      printf("reset swap to2\n");
      m_pWriteCache = m_pCache2;
    }
  }

  return m_pWriteCache->Reset(iSourcePosition, clearAnyway);
}

void CDoubleCache::EndOfInput()
{
  m_pWriteCache->EndOfInput();
}

bool CDoubleCache::IsEndOfInput()
{
  return m_pReadCache->IsEndOfInput();
}

void CDoubleCache::ClearEndOfInput()
{
  m_pWriteCache->ClearEndOfInput();
}

int64_t CDoubleCache::CachedDataEndPos()
{
  return m_pWriteCache->CachedDataEndPos(); // FIXME?
}

int64_t CDoubleCache::CachedDataBeginPos()
{
  return m_pWriteCache->CachedDataBeginPos(); // FIXME?
}

int64_t CDoubleCache::CachedDataEndPosIfSeekTo(int64_t iFilePosition)
{
  return std::max(m_pCache1->CachedDataEndPosIfSeekTo(iFilePosition), m_pCache2->CachedDataEndPosIfSeekTo(iFilePosition));
}

bool CDoubleCache::IsCachedPosition(int64_t iFilePosition)
{
  return m_pCache1->IsCachedPosition(iFilePosition) || m_pCache2->IsCachedPosition(iFilePosition);
}

CCacheStrategy *CDoubleCache::CreateNew()
{
  return new CDoubleCache(m_pCache1->CreateNew());
}
