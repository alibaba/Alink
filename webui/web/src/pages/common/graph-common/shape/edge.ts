import { Shape, Edge } from "@antv/x6";
import "./edge.less";

export class BaseEdge extends Shape.Edge {
  // eslint-disable-next-line class-methods-use-this
  isGroupEdge() {
    return false;
  }
}

export class GuideEdge extends BaseEdge {}

GuideEdge.config({
  shape: "GuideEdge",
  connector: { name: "alink" },
  zIndex: 2,
  attrs: {
    line: {
      stroke: "#808080",
      strokeWidth: 1,
      targetMarker: {
        stroke: "none",
        fill: "none",
      },
    },
  },
});

export class X6GroupEdge extends GuideEdge {
  // eslint-disable-next-line class-methods-use-this
  isGroupEdge() {
    return true;
  }
}

X6GroupEdge.config({
  shape: "X6GroupEdge",
});

Edge.registry.register({
  GuideEdge: GuideEdge as any,
  X6GroupEdge: X6GroupEdge as any,
});
