import React from "react";
import { Form, Input } from "antd";
import { useObservableState } from "@/common/hooks/useObservableState";
import { useExperimentGraph } from "@/pages/rx-models/experiment-graph";
import "antd/lib/style/index.css";
import { NodeParamForm } from "@/pages/component-config-panel/form/node-param-config";

export interface Props {
  experimentId: string;
  nodeId: string;
}

export const NodeForm: React.FC<Props> = ({ nodeId, experimentId }) => {
  const [form] = Form.useForm();

  const expGraph = useExperimentGraph(experimentId);
  const [node] = useObservableState(() => expGraph.activeNodeInstance$);

  const onValuesChange = ({ name }: { name: string }) => {
    if (node.name !== name) {
      expGraph.renameNode(nodeId, name);
    }
  };

  return (
    <React.Fragment>
      <Form
        form={form}
        layout="vertical"
        initialValues={{ name: node ? node.name : "" }}
        onValuesChange={onValuesChange}
        requiredMark={false}
      >
        <Form.Item label="节点名称" name="name">
          <Input />
        </Form.Item>
      </Form>
      {node && <NodeParamForm node={node} />}
    </React.Fragment>
  );
};
