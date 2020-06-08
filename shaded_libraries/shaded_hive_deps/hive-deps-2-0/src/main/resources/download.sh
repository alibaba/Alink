
function dl() {
  lib=$1
  v=$2
  f="datanucleus-${lib}-${v}.jar"
  targetdir="$PWD/datanucleus-${lib}"
  url="https://repo1.maven.org/maven2/org/datanucleus/datanucleus-${lib}/${v}/datanucleus-${lib}-${v}.jar"
  if [ ! -d temp ]; then mkdir temp; fi
  (
  cd temp
  if [ ! -f ${f} ]; then
    wget ${url}
  fi
  unzip ${f} -d ${lib}
  cp ${lib}/plugin.xml ${targetdir}/plugin.xml
  cp ${lib}/META-INF/MANIFEST.MF ${targetdir}/META-INF/MANIFEST.MF
  )
}

dl api-jdo 4.2.1
dl core  4.1.6
dl rdbms 4.1.7
