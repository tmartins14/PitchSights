import React from "react";
import Style from "./Field.module.css";

const Field = ({ fieldStyle }) => {
  const fieldHalf = (
    <div>
      <span className={Style.halfway_line}></span>
      <span className={Style.center_circle}></span>
      <span className={Style.center_mark}></span>
      <span className={Style.penalty_area}></span>
      <span className={Style.penalty_mark}></span>
      <span className={Style.penalty_arc}></span>
      <span className={Style.goal_area}></span>
      <span className={Style.corner_arc}></span>
    </div>
  );

  let renderedField;

  if (fieldStyle === "Field") {
    renderedField = (
      <div className={Style.field}>
        <div className={Style.left}>{fieldHalf}</div>
        <div className={Style.right}>{fieldHalf}</div>
      </div>
    );
  } else if (fieldStyle === "White Board") {
    renderedField = (
      <div className={Style.white_board}>
        <div className={Style.left}>{fieldHalf}</div>
        <div className={Style.right}>{fieldHalf}</div>
      </div>
    );
  }

  return (
    <div className={Style.body}>
      <div className={Style.container}>{renderedField}</div>
    </div>
  );
};

export default Field;
