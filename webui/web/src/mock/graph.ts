import random from "lodash/random";

interface NodeParams {
  name: string;
  x: number;
  y: number;
}

export const copyNode = ({ name, x, y }: NodeParams) => {
  const id = `${Date.now()}`;
  return {
    id,
    name,
    inPorts: [
      {
        tableName: "germany_credit_data",
        sequence: 1,
        description: "输入1",
        id: id + 100000,
      },
      {
        tableName: "germany_credit_data",
        sequence: 2,
        description: "输入2",
        id: id + 200000,
      },
    ],
    outPorts: [
      {
        tableName: "germany_credit_data",
        sequence: 1,
        description: "输出表1",
        id: id + 300000,
      },
      {
        tableName: "germany_credit_data",
        sequence: 2,
        description: "输出表2",
        id: id + 400000,
      },
    ],
    positionX: x + 200 + random(20, false),
    positionY: y + random(10, false),
    codeName: "source_11111",
    catId: 1,
    nodeDefId: 111111,
    category: "source",
    status: 3,
    groupId: 0,
  };
};
