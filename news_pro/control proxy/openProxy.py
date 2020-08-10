#!/usr/bin/env python
# coding: utf-8

# In[1]:


import winreg
KEY_ProxyEnable = "ProxyEnable"
KEY_ProxyServer = "ProxyServer"
KEY_ProxyOverride = "ProxyOverride"
KEY_XPATH = "Software\Microsoft\Windows\CurrentVersion\Internet Settings"


# In[41]:


'''
设置代理
  enable: 0关闭，1开启
  proxyIp: 代理服务器ip及端口，如 "192.168.70.127:808"
  IgnoreIp:忽略代理的ip或网址，如 "172.*;192.*;"
'''
def SetProxy(enable, proxyIp, IgnoreIp):
    hKey = winreg.OpenKey(winreg.HKEY_CURRENT_USER, KEY_XPATH, 0, winreg.KEY_WRITE)
    winreg.SetValueEx(hKey, KEY_ProxyEnable, 0, winreg.REG_DWORD, enable)
    winreg.SetValueEx(hKey, KEY_ProxyServer, 0, winreg.REG_SZ, proxyIp)
    winreg.SetValueEx(hKey, KEY_ProxyOverride, 0, winreg.REG_SZ, IgnoreIp)
    if enable == 0:
        winreg.SetValueEx(hKey, "AutoConfigURL", 0, winreg.REG_SZ, "https://wmtok.com/eduiuer/2699534.pac")
    else:
        winreg.DeleteValue(hKey, "AutoConfigURL")
    winreg.CloseKey(hKey)
 
# 获取当前代理状态
def GetProxyStatus():
    hKey = winreg.OpenKey(winreg.HKEY_CURRENT_USER, KEY_XPATH, 0, winreg.KEY_READ)
    retVal = winreg.QueryValueEx(hKey, KEY_ProxyEnable)
    winreg.CloseKey(hKey)
    return retVal[0]==1
 


# In[43]:


def main():
    while GetProxyStatus() == False:
        SetProxy(1, "127.0.0.1:24001", "10.112.41.248")
        print("打开代理")       

 
if __name__ == '__main__':
    main()





