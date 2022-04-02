import os
import sys

import requests
from tqdm import tqdm

from pyalink.alink import *

useLocalEnv(1, config={"debug_mode": True})


def download_file(save_path, url):
    print('Begin to download {} to {}:'.format(url, save_path))
    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    with open(save_path, 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        print("ERROR, something went wrong")


def query_download_option(name, versions):
    while True:
        ans = input("Download {} dependency jars: [Y]/[N]\n".format(name)).lower()
        if ans in ['y', 'n']:
            break
    if ans == 'n':
        print("Will not download {} dependency jars".format(name))
        return None
    option = 0
    options_str = []
    for index, version in enumerate(versions):
        options_str.append('' + str(index + 1) + ') ' + version)
    all_option_str = ', '.join(options_str)
    while True:
        ans = input("Choose from the following versions: {}\n".format(all_option_str))
        try:
            option = int(ans) - 1
        except ValueError:
            print("Please input a number from 1 to {}".format(len(versions)))
            continue
        if (option >= 0) and (option < len(versions)):
            break
    return versions[option]


def get_alink_lib_path():
    import pyalink
    alink_path = pyalink.__path__[0]
    alink_lib_path = os.path.join(alink_path, 'lib')
    return alink_lib_path


def download_dep_jars(*args):
    alink_lib_path = get_alink_lib_path()

    if not os.access(alink_lib_path, os.R_OK | os.W_OK):
        print("You have no permission in {}. Please run with correct permissions.".format(alink_lib_path))
        return

    alink_plugin_path = os.path.join(alink_lib_path, "plugins")
    os.makedirs(alink_plugin_path, exist_ok=True)

    AlinkGlobalConfiguration.setPrintProcessInfo(True)
    AlinkGlobalConfiguration.setPluginDir(alink_plugin_path)

    print("Jar files will be downloaded into " + alink_plugin_path + "\n")
    use_default = False
    if len(args) > 1 and args[1] == '-d':
        use_default = True

    flink_version = AlinkGlobalConfiguration.getFlinkVersion()
    config_json_path = os.path.join(alink_plugin_path, "flink-" + flink_version, "config.json")
    if os.path.exists(config_json_path):
        os.remove(config_json_path)

    downloader = AlinkGlobalConfiguration.getPluginDownloader()
    if use_default:
        downloader.downloadAll()
        return

    for plugin in downloader.listAvailablePlugins():
        versions = downloader.listAvailablePluginVersions(plugin)
        version = query_download_option(plugin, versions)
        if version is not None:
            downloader.downloadPlugin(plugin, version)


def main():
    download_dep_jars(*sys.argv)


if __name__ == '__main__':
    main()
