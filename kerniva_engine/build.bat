@echo off
REM build.bat - Build Kerniva Engine library for Windows

echo Building Kerniva Engine (release)...
cargo build --release
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

echo.
echo Build complete!
echo.
echo Library: kerniva_engine.dll   (in target\release\)
echo Header:  include\kerniva_engine.h
echo.
echo Examples:
echo   Python: cd examples\python ^&^& python kerniva_engine.py
echo   C:      gcc examples\c\example.c -Iinclude -Ltarget\release -lkerniva_engine -o example.exe
echo   Go:     Copy kernelva_engine.dll to examples\go\ ^&^& cd examples\go ^&^& go run .
echo.
echo Package layout:
echo   include\kerniva_engine.h   - C/C++ header
echo   target\release\            - compiled library
echo   examples\                  - Python / C / Go examples
