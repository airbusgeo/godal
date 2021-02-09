// Copyright 2021 Airbus Defence and Space
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cpl_port.h"
#include "cpl_string.h"
#include "cpl_vsi.h"
#include "cpl_vsi_virtual.h"
#include "godal.h"
#include "_cgo_export.h"

namespace cpl
{

    /************************************************************************/
    /*                     VSIGoFilesystemHandler                         */
    /************************************************************************/

    class VSIGoFilesystemHandler : public VSIFilesystemHandler
    {
        CPL_DISALLOW_COPY_ASSIGN(VSIGoFilesystemHandler)
    private:
        size_t m_buffer, m_cache;

    public:
        VSIGoFilesystemHandler(size_t bufferSize, size_t cacheSize);
        ~VSIGoFilesystemHandler() override;

        VSIVirtualHandle *Open(const char *pszFilename,
                               const char *pszAccess,
                               bool bSetError) override;

        int Stat(const char *pszFilename, VSIStatBufL *pStatBuf, int nFlags) override;
        char **SiblingFiles(const char *pszFilename) override;
        int HasOptimizedReadMultiRange(const char *pszPath) override;
    };

    /************************************************************************/
    /*                           VSIGoHandle                              */
    /************************************************************************/

    class VSIGoHandle : public VSIVirtualHandle
    {
        CPL_DISALLOW_COPY_ASSIGN(VSIGoHandle)
    private:
        char *m_filename;
        vsi_l_offset m_cur, m_size;
        int m_eof;

    public:
        VSIGoHandle(const char *filename, vsi_l_offset size);
        ~VSIGoHandle() override;

        vsi_l_offset Tell() override;
        int Seek(vsi_l_offset nOffset, int nWhence) override;
        size_t Read(void *pBuffer, size_t nSize, size_t nCount) override;
        int ReadMultiRange(int nRanges, void **ppData, const vsi_l_offset *panOffsets, const size_t *panSizes) override;
        VSIRangeStatus GetRangeStatus(vsi_l_offset nOffset, vsi_l_offset nLength) override;
        int Eof() override;
        int Close() override;
        size_t Write(const void *pBuffer, size_t nSize, size_t nCount) override;
        int Flush() override;
        int Truncate(vsi_l_offset nNewSize) override;
    };

    VSIGoHandle::VSIGoHandle(const char *filename, vsi_l_offset size)
    {
        m_filename = strdup(filename);
        m_cur = 0;
        m_eof = 0;
        m_size = size;
    }

    VSIGoHandle::~VSIGoHandle()
    {
        free(m_filename);
    }

    size_t VSIGoHandle::Write(const void *pBuffer, size_t nSize, size_t nCount)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Write not implemented for go handlers");
        return -1;
    }
    int VSIGoHandle::Flush() 
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Flush not implemented for go handlers");
        return -1;
    }
    int VSIGoHandle::Truncate(vsi_l_offset nNewSize) 
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Truncate not implemented for go handlers");
        return -1;
    }
    int VSIGoHandle::Seek(vsi_l_offset nOffset, int nWhence)
    {
        if (nWhence == SEEK_SET)
        {
            m_cur = nOffset;
        }
        else if (nWhence == SEEK_CUR)
        {
            m_cur += nOffset;
        }
        else
        {
            m_cur = m_size;
        }
        m_eof = 0;
        return 0;
    }

    vsi_l_offset VSIGoHandle::Tell()
    {
        return m_cur;
    }

    int VSIGoHandle::Eof()
    {
        return m_eof;
    }

    int VSIGoHandle::Close()
    {
        return 0;
    }

    size_t VSIGoHandle::Read(void *pBuffer, size_t nSize, size_t nCount)
    {
        if (nSize * nCount == 0)
        {
            return 0;
        }
        char *err = nullptr;
        size_t read = _gogdalReadCallback(m_filename, pBuffer, m_cur, nSize * nCount, &err);
        if (err)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
            errno = EIO;
            free(err);
            return 0;
        }
        if (read != nSize * nCount)
        {
            m_eof = 1;
        }
        size_t readblocks = read / nSize;
        m_cur += readblocks * nSize;
        return readblocks;
    }

    int VSIGoHandle::ReadMultiRange(int nRanges, void **ppData, const vsi_l_offset *panOffsets, const size_t *panSizes)
    {
        int iRange;
        int nMergedRanges = 1;
        for (iRange = 0; iRange < nRanges - 1; iRange++)
        {
            if (panOffsets[iRange] + panSizes[iRange] != panOffsets[iRange + 1])
            {
                nMergedRanges++;
            }
        }
        char *err = nullptr;
        if (nMergedRanges == nRanges)
        {
            int ret = _gogdalMultiReadCallback(m_filename, nRanges, (void *)ppData, (void *)panOffsets, (void *)panSizes, &err);
            if (err)
            {
                CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
                errno = EIO;
                free(err);
                return -1;
            }
            return ret;
        }

        vsi_l_offset *mOffsets = new vsi_l_offset[nMergedRanges];
        size_t *mSizes = new size_t[nMergedRanges];
        char **mData = new char *[nMergedRanges];

        int curRange = 0;
        mSizes[curRange] = panSizes[0];
        mOffsets[curRange] = panOffsets[0];
        for (iRange = 0; iRange < nRanges - 1; iRange++)
        {
            if (panOffsets[iRange] + panSizes[iRange] == panOffsets[iRange + 1])
            {
                mSizes[curRange] += panSizes[iRange + 1];
            }
            else
            {
                mData[curRange] = new char[mSizes[curRange]];
                //start a new range
                curRange++;
                mSizes[curRange] = panSizes[iRange + 1];
                mOffsets[curRange] = panOffsets[iRange + 1];
            }
        }
        mData[curRange] = new char[mSizes[curRange]];

        int ret = _gogdalMultiReadCallback(m_filename, nRanges, (void *)ppData, (void *)panOffsets, (void *)panSizes, &err);

        if (err == nullptr)
        {
            curRange = 0;
            size_t curOffset = panSizes[0];
            memcpy(ppData[0], mData[0], panSizes[0]);
            for (iRange = 0; iRange < nRanges - 1; iRange++)
            {
                if (panOffsets[iRange] + panSizes[iRange] == panOffsets[iRange + 1])
                {
                    memcpy(ppData[iRange + 1], mData[curRange] + curOffset, panSizes[iRange + 1]);
                    curOffset += panSizes[iRange + 1];
                }
                else
                {
                    curRange++;
                    memcpy(ppData[iRange + 1], mData[curRange], panSizes[iRange + 1]);
                    curOffset = panSizes[iRange + 1];
                }
            }
        }
        else
        {
            CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
            errno = EIO;
            free(err);
            ret = -1;
        }

        delete[] mOffsets;
        delete[] mSizes;
        for (int i = 0; i < nMergedRanges; i++)
        {
            delete[] mData[i];
        }
        delete[] mData;

        return ret;
    }

    VSIRangeStatus VSIGoHandle::GetRangeStatus(vsi_l_offset nOffset, vsi_l_offset nLength)
    {
        return VSI_RANGE_STATUS_UNKNOWN;
    }

    VSIGoFilesystemHandler::VSIGoFilesystemHandler(size_t bufferSize, size_t cacheSize)
    {
        m_buffer = bufferSize;
        m_cache = (cacheSize < bufferSize) ? bufferSize : cacheSize;
    }
    VSIGoFilesystemHandler::~VSIGoFilesystemHandler() {}

    VSIVirtualHandle *VSIGoFilesystemHandler::Open(const char *pszFilename,
                                                   const char *pszAccess,
                                                   bool bSetError)
    {
        if (strchr(pszAccess, 'w') != NULL ||
            strchr(pszAccess, '+') != NULL)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Only read-only mode is supported");
            return NULL;
        }
        char *err = nullptr;
        long long s = _gogdalSizeCallback((char *)pszFilename, &err);

        if (s == -1)
        {
            if (err != nullptr)
            {
                CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
            }
            errno = ENOENT;
            return NULL;
        }
        if (m_buffer == 0)
        {
            return new VSIGoHandle(pszFilename, s);
        }
        else
        {
            return VSICreateCachedFile(new VSIGoHandle(pszFilename, s), m_buffer, m_cache);
        }
    }

    int VSIGoFilesystemHandler::Stat(const char *pszFilename,
                                     VSIStatBufL *pStatBuf,
                                     int nFlags)
    {
        char *err = nullptr;
        long long s = _gogdalSizeCallback((char *)pszFilename, &err);
        if (s == -1)
        {
            if (nFlags & VSI_STAT_SET_ERROR_FLAG)
            {
                CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
                errno = ENOENT;
            }
            return -1;
        }
        memset(pStatBuf, 0, sizeof(VSIStatBufL));
        pStatBuf->st_mode = S_IFREG;

        if (nFlags & VSI_STAT_SIZE_FLAG)
        {
            pStatBuf->st_size = s;
        }
        return 0;
    }

    int VSIGoFilesystemHandler::HasOptimizedReadMultiRange(const char * /*pszPath*/)
    {
        return TRUE;
    }

    char **VSIGoFilesystemHandler::SiblingFiles(const char *pszFilename)
    {
        return (char **)calloc(1, sizeof(char *));
    }

} // namespace cpl

char* VSIInstallGoHandler(const char *pszPrefix, size_t bufferSize, size_t cacheSize)
{
    CSLConstList papszPrefix = VSIFileManager::GetPrefixes();
    for( size_t i = 0; papszPrefix && papszPrefix[i]; ++i ) {
        if(strcmp(papszPrefix[i],pszPrefix)==0) {
            return strdup("handler already registered on prefix");
        }
    }
    VSIFilesystemHandler *poHandler = new cpl::VSIGoFilesystemHandler(bufferSize, cacheSize);
    const std::string sPrefix(pszPrefix);
    VSIFileManager::InstallHandler(sPrefix, poHandler);
    return nullptr;
}