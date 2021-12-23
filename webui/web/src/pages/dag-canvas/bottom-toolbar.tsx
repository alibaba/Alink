import React, { useCallback, useEffect, useState } from "react";
import { Popover, Modal, Input } from "antd";
import {
  CloudUploadOutlined,
    CodeOutlined,
    DashboardOutlined,
  LogoutOutlined,
  PlayCircleOutlined,
} from "@ant-design/icons";
import classNames from "classnames";
import { useObservableState } from "@/common/hooks/useObservableState";
import { useExperimentGraph } from "@/pages/rx-models/experiment-graph";
import styles from "./bottom-toolbar.less";
import { exportScriptReq } from "@/requests/graph";

interface Props {
  experimentId: string;
}

export const BottomToolbar: React.FC<Props> = (props) => {
  const { experimentId } = props;
  const expGraph = useExperimentGraph(experimentId);
  const [running] = useObservableState(expGraph.running$);
  const [preparingRun, setPreparingRun] = useState(false);
  const [preparingStop, setPreparingStop] = useState(false);

  // running 的值发生变化，说明运行或停止按钮的操作产生了作用
  useEffect(() => {
    setPreparingRun(false);
    setPreparingStop(false);
  }, [running]);

  // 运行实验
  const onRunExperiment = useCallback(() => {
    setPreparingRun(true);
    expGraph.runGraph().then((res: any) => {
      if (!res.success) {
        setPreparingRun(false);
      }
    });
  }, [expGraph]);

  // 停止运行
  const onStopRunExperiment = useCallback(() => {
    setPreparingStop(true);
    expGraph.stopRunGraph().then((res: any) => {
      if (!res.success) {
        setPreparingStop(false);
      }
    });
  }, [expGraph]);

  const [isModalVisible, setIsModalVisible] = useState(false);
  const [code, setCode] = useState("");

  const onExportScript = useCallback(async () => {
    const { lines }: { lines: string[] } = await exportScriptReq();
    setIsModalVisible(true);
    setCode(lines.join("\n"));
  }, [expGraph]);

  const handleModalOk = () => {
    setIsModalVisible(false);
  };

  const handleModalCancel = () => {
    setIsModalVisible(false);
  };

  const openFlinkUI = () => {
    window.open("http://localhost:8081", "_blank");
  };

  const openNotebook = () => {
    window.open("http://localhost:8888", "_blank");
  };

  const runningConfigs = [
    {
      content: "运行",
      tip: "依次运行本实验的每个组件",
      icon: PlayCircleOutlined,
      disabled: preparingRun,
      clickHandler: onRunExperiment,
    },
    {
      content: "停止",
      tip: "停止运行实验",
      icon: LogoutOutlined,
      disabled: preparingStop,
      clickHandler: onStopRunExperiment,
    },
  ];

  const runningConfig = runningConfigs[Number(!!running)];
  const RunningIcon = runningConfig.icon;

  const exportConfig = {
    content: "导出",
    tip: "导出 PyAlink 脚本",
    icon: CloudUploadOutlined,
    clickHandler: onExportScript,
  };

  const flinkUIConfig = {
    content: "FlinkUI",
    tip: "FlinkUI",
    icon: DashboardOutlined,
    clickHandler: openFlinkUI,
  };

  const notebookConfig = {
    content: "Notebook",
    tip: "Notebook",
    icon: CodeOutlined,
    clickHandler: openNotebook,
  };

  return (
    <div className={styles.bottomToolbar}>
      <ul className={styles.itemList}>
        {/* 部署 */}
        <Popover content={exportConfig.tip} overlayClassName={styles.popover}>
          <li className={styles.item} onClick={exportConfig.clickHandler}>
            <CloudUploadOutlined />
            <span>{exportConfig.content}</span>
          </li>
        </Popover>

        {/* 运行/停止 */}
        <Popover content={runningConfig.tip} overlayClassName={styles.popover}>
          <li
            className={classNames(styles.item, {
              [styles.disabled]: runningConfig.disabled,
            })}
            onClick={runningConfig.clickHandler}
          >
            <RunningIcon />
            <span>{runningConfig.content}</span>
          </li>
        </Popover>

        {/* Flink webui */}
        <Popover content={flinkUIConfig.tip} overlayClassName={styles.popover}>
          <li className={styles.item} onClick={flinkUIConfig.clickHandler} >
            <DashboardOutlined />
            <span>{flinkUIConfig.content}</span>
          </li>
        </Popover>

        {/* Notebook */}
        <Popover content={notebookConfig.tip} overlayClassName={styles.popover}>
          <li className={styles.item} onClick={notebookConfig.clickHandler} >
            <CodeOutlined />
            <span>{notebookConfig.content}</span>
          </li>
        </Popover>
      </ul>
      <Modal
        title="导出 PyAlink 脚本"
        visible={isModalVisible}
        onOk={handleModalOk}
        onCancel={handleModalCancel}
      >
        <Input.TextArea
          autoSize={{ minRows: 2, maxRows: 15 }}
          value={code}
        />
      </Modal>
    </div>
  );
};
