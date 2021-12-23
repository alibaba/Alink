set -e

PYTHON_HOME=$1

echo "PYTHON_HOME = " "$PYTHON_HOME"
cd "$PYTHON_HOME"
source bin/activate
bin/conda-unpack
