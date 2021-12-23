@echo off

echo windows call conda unpack script
echo %PYTHON_HOME%

cd %PYTHON_HOME%

Scripts/activate.bat
Scripts/conda_unpack.exe