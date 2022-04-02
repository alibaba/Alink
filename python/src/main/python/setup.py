import io
import os

import setuptools

print(setuptools.find_packages())

version_path = os.path.join("pyalink", "alink", "version.py")
# For Python 3 compatibility, we can't use execfile; this is 2to3's conversion:
exec(compile(open(version_path).read(), version_path, "exec"))
version = __version__  # noqa

with open('requirements.txt') as fp:
    install_requires = fp.read()

this_directory = os.path.abspath(os.path.dirname(__file__))
with io.open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name="pyalink-flink-1.12",
    version=version,
    author="xuyang1706,chengscu,Fanoid,hapsunday,liulfy,lqb11,Xiafei Qiu,shaomengwang,cainingnk,weibo.zwb",
    author_email="xuyang1706@gmail.com,chengscu@qq.com,hongfanxo@gmail.com,hapsunday@gmail.com,2691140740@qq.com,liqianbing11@163.com,qiuxiafei@gmail.com,shaomeng.wang.w@gmail.com,cainingnk@gmail.com,weibo.zwb@alibaba-inc.com",
    description="Alink Python API",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/alibaba/Alink",
    license="Apache License, Version 2.0",
    packages=setuptools.find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
    ],
    entry_points={
        'console_scripts': [
            'download_pyalink_dep_jars=pyalink.alink.download_pyalink_dep_jars:main'
        ]
    },
    python_requires='>=3.6',
    install_requires=install_requires,
    include_package_data=True
)
