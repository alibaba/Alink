import { get, post } from "./request";
import { getAlgoConfig } from "@/requests/algo";
import { range } from "lodash-es";

interface NodeParams {
  name: string;
  className: string;
  x: number;
  y: number;
}

const inPortPrefix = "in";
const outPortPrefix = "out";

export const getNodeReq = async (id: number | string) => {
  return await get(`/node/get?node_id=${id}`);
};

export const convertNode = (node: any) => {
  const config = getAlgoConfig(node.className);
  node.inPorts = range(config.numInPorts).map((_, index) => ({
    sequence: index + 1,
    id: `${inPortPrefix}-${node.id}-${index}`,
  }));
  node.outPorts = range(config.numOutPorts).map((_, index) => ({
    sequence: index + 1,
    id: `${outPortPrefix}-${node.id}-${index}`,
  }));
  node.nodeInstanceId = node.id;
  return node;
};

export const convertEdge = (edge: any) => {
  edge.source = edge.srcNodeId;
  edge.outputPortId = `${outPortPrefix}-${edge.source}-${edge.srcNodePort}`;
  edge.target = edge.dstNodeId;
  edge.inputPortId = `${inPortPrefix}-${edge.target}-${edge.dstNodePort}`;
  return edge;
};

export const convertGraph = (graph: any) => {
  graph.nodes = graph.nodes.map((d: any) => convertNode(d));
  graph.links = graph.edges.map((d: any) => convertEdge(d));
  delete graph.edges;
  return graph;
};

// Node requests
export const addNodeReq = async ({ name, className, x, y }: NodeParams) => {
  const data = {
    nodeType: "FUNCTION",
    nodeName: name,
    positionX: x,
    positionY: y,
    className: className,
  };
  return await post("/node/add", data);
};

export const moveNodeReq = async (id: number, x: number, y: number) => {
  await get(`/node/update?node_id=${id}&position_x=${x}&position_y=${y}`);
};

export const delNodeReq = async (nodeId: number) => {
  return await get(`/node/del?node_id=${nodeId}`);
};

export const renameNodeReq = async (nodeId: number | string, name: string) => {
  return await get(`/node/update?node_id=${nodeId}&name=${name}`);
};

// Edge requests
export const addEdgeReq = async (
  source: string,
  target: string,
  outputPortId: string,
  inputPortId: string
) => {
  console.assert(outputPortId.startsWith(outPortPrefix));
  console.assert(inputPortId.startsWith(inPortPrefix));

  const srcNodeId = parseInt(source);
  const dstNodeId = parseInt(target);
  const srcNodePort = parseInt(outputPortId.split("-")[2]);
  const dstNodePort = parseInt(inputPortId.split("-")[2]);

  const data = {
    srcNodeId: srcNodeId,
    srcNodePort: srcNodePort,
    dstNodeId: dstNodeId,
    dstNodePort: dstNodePort,
  };
  return await post("/edge/add", data);
};

export const delEdgeReq = async (edgeId: number) => {
  return await get(`/edge/del?edge_id=${edgeId}`);
};

// Node param requests
export const getNodeParamReq = async (nodeId: number) => {
  const res = await get(`/param/get?node_id=${nodeId}`);
  console.warn(`res = ${JSON.stringify(res)}`);
  return res;
};

export const updateNodeParamReq = async (
  nodeId: number,
  paramsToUpdate: any,
  paramsToDel: string[]
) => {
  const data = {
    nodeId: nodeId,
    paramsToUpdate: paramsToUpdate,
    paramsToDel: paramsToDel,
  };
  return await post("/param/update", data);
};

// Experiment requests
export const queryGraphReq = async () => {
  const data: any = await get("/experiment/get_graph");
  return {
    lang: "zh_CN",
    success: true,
    data: data,
    Lang: "zh_CN",
  };
};

export const updateExperimentReq = async (config: string) => {
  return await post("/experiment/update", {
    config: config,
  });
};

export const getExperimentReq = async () => {
  return await get("/experiment/get");
};

export const runGraphReq = async (experimentId: number | string) => {
  return await get(`/experiment/run?experimentId=${experimentId}`);
};

export const exportScriptReq = async () => {
  return await get("/experiment/export_pyalink_script");
};
