/*
 *      Copyright (C) 2005-2013 Team XBMC
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
#include "ScreenSaver.h"
#include "guilib/GraphicContext.h"
#include "interfaces/generic/ScriptInvocationManager.h"
#include "settings/Settings.h"
#include "windowing/WindowingFactory.h"

namespace ADDON
{

CScreenSaver::CScreenSaver(const char *addonID)
    : ADDON::CAddonDll<DllScreenSaver, ScreenSaver, SCR_PROPS>(AddonProps(addonID, ADDON_UNKNOWN))
{
}

bool CScreenSaver::IsInUse() const
{
  return CSettings::GetInstance().GetString(CSettings::SETTING_SCREENSAVER_MODE) == ID();
}

bool CScreenSaver::CreateScreenSaver()
{
  printf("libname = %s\n", LibPath().c_str());
  if (CScriptInvocationManager::GetInstance().HasLanguageInvoker(LibPath()))
  {
    if (!CScriptInvocationManager::GetInstance().Stop(LibPath()))
      CScriptInvocationManager::GetInstance().ExecuteAsync(LibPath(), AddonPtr(new CScreenSaver(*this)));
    return true;
  }
 // pass it the screen width,height
 // and the name of the screensaver
  int iWidth = g_graphicsContext.GetWidth();
  int iHeight = g_graphicsContext.GetHeight();

  m_pInfo = new SCR_PROPS;
#ifdef HAS_DX
  m_pInfo->device     = g_Windowing.Get3D11Context();
#else
  m_pInfo->device     = NULL;
#endif
  m_pInfo->x          = 0;
  m_pInfo->y          = 0;
  m_pInfo->width      = iWidth;
  m_pInfo->height     = iHeight;
  m_pInfo->pixelRatio = g_graphicsContext.GetResInfo().fPixelRatio;
  m_pInfo->name       = strdup(Name().c_str());
  m_pInfo->presets    = strdup(CSpecialProtocol::TranslatePath(Path()).c_str());
  m_pInfo->profile    = strdup(CSpecialProtocol::TranslatePath(Profile()).c_str());

  if (CAddonDll<DllScreenSaver, ScreenSaver, SCR_PROPS>::Create() == ADDON_STATUS_OK)
    return true;

  return false;
}

void CScreenSaver::Start()
{
  // notify screen saver that they should start
  if (Initialized()) m_pStruct->Start();
}

void CScreenSaver::Render()
{
  // ask screensaver to render itself
  if (Initialized()) m_pStruct->Render();
}

void CScreenSaver::GetInfo(SCR_INFO *info)
{
  // get info from screensaver
  if (Initialized()) m_pStruct->GetInfo(info);
}

void CScreenSaver::Destroy()
{
#ifdef HAS_PYTHON
  if (URIUtils::HasExtension(LibPath(), ".py"))
  {
    // FIXME: We need to move this out of here, we can only terminate if the window was changed
    //g_alarmClock.Start(SCRIPT_ALARM, SCRIPT_TIMEOUT, "StopScript(" + LibPath() + ")", true, false);
    printf("screensaver::destroy\n");
    CScriptInvocationManager::GetInstance().Stop(LibPath(), false);
    return;
  }
#endif
  // Release what was allocated in method CScreenSaver::CreateScreenSaver.
  if (m_pInfo)
  {
    free((void *) m_pInfo->name);
    free((void *) m_pInfo->presets);
    free((void *) m_pInfo->profile);

    delete m_pInfo;
    m_pInfo = NULL;
  }

  CAddonDll<DllScreenSaver, ScreenSaver, SCR_PROPS>::Destroy();
}

} /*namespace ADDON*/

