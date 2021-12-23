import React, { useEffect } from "react";
import { Form, Input } from "antd";
import { getNodeParamReq, updateNodeParamReq } from "@/requests/graph";
import { getAlgoConfig } from "@/requests/algo";

export interface Props {
  node: { id: number; className: string };
}

export const NodeParamForm: React.FC<Props> = ({ node }: Props) => {
  const [form] = Form.useForm();

  const onValuesChange = (data: any) => {
    updateNodeParamReq(node.id, data, []);
  };

  useEffect(() => {
    if (node?.id) {
      getNodeParamReq(node.id).then((d) => {
        let paramMap: { [name: string]: string | null } = {};
        for (const { key, value } of d.parameters) {
          paramMap[key] = value;
        }
        form.setFieldsValue(paramMap);
      });
    }
  }, [node]);

  const config = getAlgoConfig(node.className);
  const paramNames: string[] = config.params;

  return (
    <Form form={form} layout="vertical" onValuesChange={onValuesChange}>
      {paramNames.map((name) => (
        <Form.Item key={name} name={name} label={name}>
          <Input placeholder="NOT SET" />
        </Form.Item>
      ))}
    </Form>
  );
};
