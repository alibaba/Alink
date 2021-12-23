/* eslint-disable no-param-reassign */
import { useCallback, useState } from "react";
import { algoData } from "@/requests/algo-config";
import { searchByKeyword } from "@/requests/algo";

export default () => {
  const [keyword, setKeyword] = useState<string>(""); // 搜索关键字
  const [loading, setLoading] = useState<boolean>(false); // 加载状态
  const [componentTreeNodes, setComponentTreeNodes] = useState<any[]>([]);
  const [searchList, setSearchList] = useState<any[]>([]); // 搜索结果列表

  // 加载组件
  const loadComponentNodes = useCallback(() => {
    setLoading(true);
    const load = async () => {
      try {
        if (algoData) {
          setComponentTreeNodes(algoData);
        }
      } finally {
        setLoading(false);
      }
    };

    return load();
  }, []);

  // 搜索组件
  const search = useCallback((params: { keyword: string }) => {
    setKeyword(params.keyword ? params.keyword : "");
    if (!params.keyword) {
      return;
    }
    setLoading(true);

    const load = async () => {
      try {
        const nodes = ([] = await searchByKeyword(params.keyword));
        setSearchList(nodes);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  return {
    // 状态
    keyword,
    loading,
    componentTreeNodes,
    searchList,

    // 方法
    setKeyword,
    loadComponentNodes,
    search,
  };
};
