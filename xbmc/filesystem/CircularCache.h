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

#ifndef CACHECIRCULAR_H
#define CACHECIRCULAR_H

#include "CacheStrategy.h"
#include "threads/CriticalSection.h"
#include "threads/Event.h"

namespace XFILE {

class CCircularCache : public CCacheStrategy
{
public:
    CCircularCache(size_t front, size_t back, bool bDoubleCache);
    virtual ~CCircularCache();

    virtual int Open() ;
    virtual void Close();

    virtual size_t GetMaxWriteSize(const size_t& iRequestSize) ;
    virtual int WriteToCache(const char *buf, size_t len) ;
    virtual int ReadFromCache(char *buf, size_t len) ;
    virtual int64_t WaitForData(unsigned int minimum, unsigned int iMillis) ;

    virtual int64_t Seek(int64_t pos) ;
    virtual void Reset(int64_t pos, bool clearAnyway=true) ;

    virtual int64_t CachedDataEndPosIfSeekTo(int64_t iFilePosition);
    virtual int64_t CachedDataEndPos(); 
    virtual bool IsCachedPosition(int64_t iFilePosition);

    virtual CCacheStrategy *CreateNew();
protected:
    int64_t GetAvailableData();
    bool IsCached1Position(int64_t iFilePosition);
    bool IsCached2Position(int64_t iFilePosition);

    int64_t           m_beg1;         /**< index in file (not buffer) of beginning of valid data */
    int64_t           m_end1;         /**< index in file (not buffer) of end of valid data */
    unsigned          m_time1;        /**< The last timestamp this cache was used */
    size_t            m_start1;       /**< absolute position in the cache where cache 1 starts (cache 2 ends at m_border - 1) */
    int64_t           m_beg2;         /**< index in file (not buffer) of beginning of valid data */
    int64_t           m_end2;         /**< index in file (not buffer) of end of valid data */
    unsigned          m_time2;        /**< The last timestamp this cache was used */
    size_t            m_start2;       /**< absolute position in the cache where cache 1 starts (cache 2 ends at m_border - 1) */
    int64_t           m_readPos;      /**< current reading index in file */
    int64_t           m_writePos;     /**< current writing index in file */
    uint8_t          *m_buf;          /**< buffer holding data */
    size_t            m_size;         /**< size of data buffer used (m_buf) */
    size_t            m_size_back;    /**< guaranteed size of back buffer (actual size can be smaller, or larger if front buffer doesn't need it) */
    bool              m_double_cache;
    CCriticalSection  m_sync;
    CEvent            m_written;
#ifdef TARGET_WINDOWS
    HANDLE            m_handle;
#endif
};

} // namespace XFILE
#endif
