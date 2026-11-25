Unicode true
Name "Kerniva Workbench"
OutFile "${INSTALLER_OUT}"
InstallDir "$LOCALAPPDATA\Programs\Kerniva Workbench"
RequestExecutionLevel user
SetCompressor /SOLID lzma
ShowInstDetails show
ShowUninstDetails show

!define PRODUCT_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\KernivaWorkbench"
!define LOGIN_KEY "Software\Microsoft\Windows\CurrentVersion\Run"
!define INSTALL_MARKER ".kerniva-workbench-install"
!include LogicLib.nsh

Page directory
Page instfiles
UninstPage uninstConfirm
UninstPage instfiles

Function .onInit
  SetShellVarContext current
  ReadRegStr $0 HKCU "${PRODUCT_KEY}" "InstallLocation"
  ${If} $0 != ""
    MessageBox MB_OK|MB_ICONSTOP "Uninstall the existing Kerniva Workbench before installing this release." /SD IDOK
    SetErrorLevel 1
    Abort
  ${EndIf}
FunctionEnd

Section "Kerniva Workbench"
  ${If} ${FileExists} "$INSTDIR\*"
    MessageBox MB_OK|MB_ICONSTOP "Choose a new installation directory." /SD IDOK
    SetErrorLevel 1
    Abort
  ${EndIf}
  SetOutPath "$INSTDIR"
  File /r "${BUNDLE_DIR}\*"
  ClearErrors
  FileOpen $0 "$INSTDIR\${INSTALL_MARKER}" w
  IfErrors marker_failed
  FileWrite $0 "${INSTALL_BUILD_ID}"
  FileClose $0
  IfErrors marker_failed
  WriteUninstaller "$INSTDIR\Uninstall.exe"
  CreateDirectory "$SMPROGRAMS\Kerniva Workbench"
  CreateShortcut "$SMPROGRAMS\Kerniva Workbench\Kerniva Workbench.lnk" "$INSTDIR\Kerniva Workbench.exe"
  CreateShortcut "$SMPROGRAMS\Kerniva Workbench\Uninstall.lnk" "$INSTDIR\Uninstall.exe"
  WriteRegStr HKCU "${PRODUCT_KEY}" "DisplayName" "Kerniva Workbench"
  WriteRegStr HKCU "${PRODUCT_KEY}" "DisplayVersion" "${APP_VERSION}"
  WriteRegStr HKCU "${PRODUCT_KEY}" "Publisher" "Kerniva"
  WriteRegStr HKCU "${PRODUCT_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKCU "${PRODUCT_KEY}" "InstallBuildId" "${INSTALL_BUILD_ID}"
  WriteRegStr HKCU "${PRODUCT_KEY}" "UninstallString" '$\"$INSTDIR\Uninstall.exe$\"'
  ; Start the temporary uninstaller directly to preserve its exit status in quiet mode.
  StrCpy $1 `"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -Command "`
  StrCpy $2 `$$ErrorActionPreference='Stop';$$root=(Get-ItemProperty -LiteralPath 'HKCU:${PRODUCT_KEY}' -Name InstallLocation).InstallLocation;$$temp=Join-Path ([IO.Path]::GetTempPath()) ('kerniva-uninstall-'+[guid]::NewGuid().ToString('N'));[IO.Directory]::CreateDirectory($$temp)|Out-Null;$$copy=Join-Path $$temp 'Uninstall.exe';try{Copy-Item -LiteralPath (Join-Path $$root 'Uninstall.exe') -Destination $$copy;$$code=(Start-Process -FilePath $$copy -ArgumentList @('/S',('_?='+$$root)) -Wait -PassThru).ExitCode}finally{[IO.File]::Delete($$copy);[IO.Directory]::Delete($$temp)};exit $$code`
  WriteRegStr HKCU "${PRODUCT_KEY}" "QuietUninstallString" `$1$2"`
  WriteRegDWORD HKCU "${PRODUCT_KEY}" "NoModify" 1
  WriteRegDWORD HKCU "${PRODUCT_KEY}" "NoRepair" 1
  Goto installed
marker_failed:
  MessageBox MB_OK|MB_ICONSTOP "The installation identity could not be written." /SD IDOK
  SetErrorLevel 1
  Abort
installed:
SectionEnd

Function un.onInit
  SetShellVarContext current
  ReadRegStr $0 HKCU "${PRODUCT_KEY}" "InstallLocation"
  StrCmpS $0 "$INSTDIR" 0 invalid_install
  ReadRegStr $0 HKCU "${PRODUCT_KEY}" "InstallBuildId"
  StrCmpS $0 "${INSTALL_BUILD_ID}" 0 invalid_install
  ClearErrors
  FileOpen $1 "$INSTDIR\${INSTALL_MARKER}" r
  IfErrors invalid_install
  FileRead $1 $0
  FileClose $1
  IfErrors invalid_install
  StrCmpS $0 "${INSTALL_BUILD_ID}" valid_install
invalid_install:
  MessageBox MB_OK|MB_ICONSTOP "This uninstaller does not match the registered installation." /SD IDOK
  SetErrorLevel 1
  Abort
valid_install:
FunctionEnd

!macro RemovePayload path
  ClearErrors
  Delete "${path}"
  IfErrors removal_failed
!macroend

Section "Uninstall"
  !include "${UNINSTALL_FILES}"
  !insertmacro RemovePayload "$INSTDIR\Uninstall.exe"
  Delete "$INSTDIR\${INSTALL_MARKER}"
  DeleteRegKey HKCU "${PRODUCT_KEY}"
  ReadRegStr $0 HKCU "${LOGIN_KEY}" "Kerniva Workbench"
  StrCmpS $0 '$\"$INSTDIR\Kerniva Workbench.exe$\"' 0 +2
    DeleteRegValue HKCU "${LOGIN_KEY}" "Kerniva Workbench"
  Delete "$SMPROGRAMS\Kerniva Workbench\Kerniva Workbench.lnk"
  Delete "$SMPROGRAMS\Kerniva Workbench\Uninstall.lnk"
  RMDir "$SMPROGRAMS\Kerniva Workbench"
  RMDir "$INSTDIR"
  Goto removed
removal_failed:
  MessageBox MB_OK|MB_ICONSTOP "Kerniva Workbench files could not be removed. Close the workbench and retry Uninstall." /SD IDOK
  SetErrorLevel 1
  Abort
removed:
SectionEnd
