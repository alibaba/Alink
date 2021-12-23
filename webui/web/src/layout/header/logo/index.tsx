import React from "react";
import css from "./index.less";

interface Props {
  border?: boolean;
}

export const SimpleLogo: React.FC<Props> = ({ border }) => {
  return (
    <div className={`${css.root} `}>
      <div className={`${css.logo}`}>Alink</div>
    </div>
  );
};
