import { algoData } from "@/requests/algo-config";

const getAllAlgoConfigsImpl: (children: any[]) => any[] = (children: any[]) => {
  const childrenConfigs: any[] = children.map((d) => {
    return d.isDir ? getAllAlgoConfigsImpl(d.children) : [d];
  });
  return [].concat(...childrenConfigs);
};

const algoConfigList = getAllAlgoConfigsImpl(algoData);

const buildClassNameConfigMap = (algoConfigList: any[]) => {
  const m = new Map<string, any>();
  algoConfigList.forEach((d) => {
    m.set(d.className, d);
  });
  return m;
};

const className2Config = buildClassNameConfigMap(algoConfigList);

export const findAlgoConfigImpl = (className: string, root: any[]) => {
  for (const d of root) {
    if ("children" in d) {
      const ret: any = findAlgoConfigImpl(className, d.children);
      if (ret) {
        return ret;
      }
    } else {
      if (className == d?.className) {
        return d;
      }
    }
  }
};

export const getAlgoConfig = (className: string) => {
  return className2Config.get(className);
};

export const searchByKeyword = async (keyword: string) => {
  return algoConfigList.filter((d) =>
    d.className.toLowerCase().includes(keyword.toLowerCase())
  );
};
